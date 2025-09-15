from __future__ import annotations

import argparse
import sys
import time

from _common import get_redis_client


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Idempotent consumer using XREADGROUP + processed set")
    p.add_argument("--stream", required=True, help="Stream key")
    p.add_argument("--group", required=True, help="Group name")
    p.add_argument("--consumer", required=True, help="Consumer name")
    p.add_argument("--block", type=int, default=5000, help="Block milliseconds (default: 5000)")
    p.add_argument("--count", type=int, default=10, help="Max entries per read")
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

    print(f"[idem-worker] stream={args.stream} group={args.group} consumer={args.consumer}")
    while True:
        data = r.xreadgroup(groupname=args.group, consumername=args.consumer,
                             streams={args.stream: ">"}, count=args.count, block=args.block)
        if not data:
            print("[xreadgroup] timeout (no entries)")
            continue
        for stream_key, entries in data:
            for entry_id, fields in entries:
                # Idempotency: skip if already processed
                if r.sismember(processed_key, entry_id):
                    print(f"[skip-duplicate] id={entry_id}")
                    r.xack(stream_key, args.group, entry_id)
                    continue

                print(f"[work] id={entry_id} fields={fields}")
                try:
                    process(fields, delay=args.sleep)
                    r.sadd(processed_key, entry_id)
                    r.xack(stream_key, args.group, entry_id)
                    print(f"[ack] id={entry_id}")
                except Exception as e:
                    print(f"[error] id={entry_id} err={e}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)

