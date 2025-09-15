from __future__ import annotations

import argparse
import sys
import time

from _common import get_redis_client


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Auto-claim reclaimer with pipelined ACK/mark/dead-letter")
    p.add_argument("--stream", required=True, help="Stream key")
    p.add_argument("--group", required=True, help="Group name")
    p.add_argument("--consumer", required=True, help="Consumer name for claiming")
    p.add_argument("--min-idle", type=int, default=60000, help="Min idle (ms) for XAUTOCLAIM")
    p.add_argument("--count", type=int, default=10, help="Batch size for XAUTOCLAIM")
    p.add_argument("--max-retries", type=int, default=5, help="After this, dead-letter")
    p.add_argument("--dead-stream", default=None, help="Dead-letter stream key (default dead:{stream})")
    p.add_argument("--sleep", type=float, default=0.0, help="Simulated processing seconds per entry")
    p.add_argument("--batch", type=int, default=50, help="Pipeline batch size before execute")
    p.add_argument("--url", default=None, help="Redis URL")
    return p.parse_args()


def process(fields: dict[str, str], delay: float = 0.0) -> None:
    if delay > 0:
        time.sleep(delay)


def main() -> None:
    args = parse_args()
    r = get_redis_client(args.url)

    processed_key = f"proc:{args.stream}"
    attempts_key = f"attempts:{args.stream}:{args.group}"
    dead_stream = args.dead_stream or f"dead:{args.stream}"

    pipe = r.pipeline(transaction=False)
    pending_ops = 0

    def flush():
        nonlocal pending_ops
        if pending_ops > 0:
            pipe.execute()
            pending_ops = 0

    start_id = "0-0"
    try:
        while True:
            try:
                next_id, entries = r.xautoclaim(
                    name=args.stream,
                    groupname=args.group,
                    consumername=args.consumer,
                    min_idle_time=args.min_idle,
                    start_id=start_id,
                    count=args.count,
                )
            except Exception as e:
                print(f"[xautoclaim-error] {e}")
                time.sleep(1.0)
                continue

            if not entries:
                start_id = "0-0"
                flush()
                time.sleep(0.5)
                continue

            for entry_id, fields in entries:
                # update attempts synchronously to branch logic
                attempts = r.hincrby(attempts_key, entry_id, 1)
                if attempts > args.max_retries:
                    try:
                        fields_with_meta = {**fields, "reason": "max_retries", "attempts": str(attempts)}
                    except Exception:
                        fields_with_meta = {"reason": "max_retries", "attempts": str(attempts)}
                    pipe.xadd(dead_stream, fields_with_meta)
                    pipe.xack(args.stream, args.group, entry_id)
                    pending_ops += 2
                    print(f"[dead-letter] id={entry_id} -> {dead_stream} attempts={attempts}")
                    continue

                # idempotent by entry id
                if r.sismember(processed_key, entry_id):
                    pipe.xack(args.stream, args.group, entry_id)
                    pending_ops += 1
                    print(f"[skip-duplicate] id={entry_id} attempts={attempts}")
                    continue

                print(f"[reclaim-work] id={entry_id} attempts={attempts} fields={fields}")
                try:
                    process(fields, delay=args.sleep)
                    pipe.sadd(processed_key, entry_id)
                    pipe.xack(args.stream, args.group, entry_id)
                    pending_ops += 2
                except Exception as e:
                    print(f"[error] id={entry_id} err={e}")

                if pending_ops >= args.batch:
                    flush()

            start_id = next_id or "0-0"
    finally:
        flush()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)

