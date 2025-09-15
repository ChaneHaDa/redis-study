from __future__ import annotations

import argparse
import sys
import time

from _common import get_redis_client


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Auto-claim idle pending messages, process idempotently, ACK or dead-letter")
    p.add_argument("--stream", required=True, help="Stream key")
    p.add_argument("--group", required=True, help="Group name")
    p.add_argument("--consumer", required=True, help="Consumer name used for claiming")
    p.add_argument("--min-idle", type=int, default=60000, help="Min idle time (ms) for XAUTOCLAIM (default: 60000)")
    p.add_argument("--count", type=int, default=10, help="Batch size for XAUTOCLAIM (default: 10)")
    p.add_argument("--max-retries", type=int, default=5, help="After this, send to dead-letter and ACK (default: 5)")
    p.add_argument("--dead-stream", default=None, help="Dead-letter stream key (default: dead:{stream})")
    p.add_argument("--sleep", type=float, default=0.0, help="Simulated processing seconds per entry")
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

    print(
        f"[reclaimer] stream={args.stream} group={args.group} consumer={args.consumer} "
        f"min_idle={args.min_idle}ms count={args.count} max_retries={args.max_retries} dead={dead_stream}"
    )

    # XAUTOCLAIM scanning cursor
    start_id = "0-0"
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
            # reset scan to cover entire PEL next time, then short sleep
            start_id = "0-0"
            time.sleep(0.5)
            continue

        for entry_id, fields in entries:
            # Increment delivery attempts in a side hash
            attempts = r.hincrby(attempts_key, entry_id, 1)
            if attempts > args.max_retries:
                # Dead-letter and ACK original
                try:
                    fields_with_meta = {**fields, "reason": "max_retries", "attempts": str(attempts)}
                except Exception:
                    fields_with_meta = {"reason": "max_retries", "attempts": str(attempts)}
                new_id = r.xadd(dead_stream, fields_with_meta)
                r.xack(args.stream, args.group, entry_id)
                print(f"[dead-letter] id={entry_id} -> {dead_stream}:{new_id} attempts={attempts}")
                continue

            # Idempotent check by stream entry id
            if r.sismember(processed_key, entry_id):
                r.xack(args.stream, args.group, entry_id)
                print(f"[skip-duplicate] id={entry_id} attempts={attempts}")
                continue

            print(f"[reclaim-work] id={entry_id} attempts={attempts} fields={fields}")
            try:
                process(fields, delay=args.sleep)
                r.sadd(processed_key, entry_id)
                r.xack(args.stream, args.group, entry_id)
                print(f"[ack] id={entry_id}")
            except Exception as e:
                print(f"[error] id={entry_id} err={e}")

        # Continue scanning from returned cursor
        start_id = next_id or "0-0"


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)

