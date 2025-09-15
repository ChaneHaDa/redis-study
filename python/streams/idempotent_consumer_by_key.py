from __future__ import annotations

import argparse
import sys
import time

from _common import get_redis_client


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Idempotent consumer by business key (e.g., order_id)")
    p.add_argument("--stream", required=True, help="Stream key")
    p.add_argument("--group", required=True, help="Group name")
    p.add_argument("--consumer", required=True, help="Consumer name")
    p.add_argument("--field", default="order_id", help="Business key field name (default: order_id)")
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

    processed_by_key = f"proc-key:{args.stream}:{args.field}"

    print(
        f"[idem-key-worker] stream={args.stream} group={args.group} consumer={args.consumer} field={args.field}"
    )
    while True:
        data = r.xreadgroup(groupname=args.group, consumername=args.consumer,
                             streams={args.stream: ">"}, count=args.count, block=args.block)
        if not data:
            print("[xreadgroup] timeout (no entries)")
            continue
        for stream_key, entries in data:
            for entry_id, fields in entries:
                keyval = fields.get(args.field)
                if keyval is None:
                    # If no business key, fall back to entry id
                    keyval = entry_id

                # First come, first serve: try reserve the key for processing
                reserved = r.sadd(processed_by_key, keyval)
                if reserved == 0:
                    # Already processed; just ack and move on
                    r.xack(stream_key, args.group, entry_id)
                    print(f"[skip-duplicate-key] id={entry_id} {args.field}={keyval}")
                    continue

                print(f"[work] id={entry_id} {args.field}={keyval} fields={fields}")
                try:
                    process(fields, delay=args.sleep)
                    r.xack(stream_key, args.group, entry_id)
                    print(f"[ack] id={entry_id}")
                except Exception as e:
                    # On error, release reservation to allow retry
                    r.srem(processed_by_key, keyval)
                    print(f"[error] id={entry_id} err={e}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)

