from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from _common import get_redis_client


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Consumer group using Lua to atomically mark-by-key + ACK")
    p.add_argument("--stream", required=True, help="Stream key")
    p.add_argument("--group", required=True, help="Group name")
    p.add_argument("--consumer", required=True, help="Consumer name")
    p.add_argument("--field", default="order_id", help="Business key field name (default: order_id)")
    p.add_argument("--count", type=int, default=20, help="Max entries per read")
    p.add_argument("--block", type=int, default=5000, help="Block milliseconds (default: 5000)")
    p.add_argument("--sleep", type=float, default=0.0, help="Simulated processing seconds per entry")
    p.add_argument("--url", default=None, help="Redis URL")
    return p.parse_args()


def process(fields: dict[str, str], delay: float = 0.0) -> None:
    if delay > 0:
        time.sleep(delay)


def main() -> None:
    args = parse_args()
    r = get_redis_client(args.url)

    processed_key = f"proc-key:{args.stream}:{args.field}"

    # Load Lua script
    script_path = Path(__file__).with_name('lua').joinpath('ack_and_mark_by_key.lua')
    script_text = script_path.read_text(encoding='utf-8')
    ack_mark_key = r.register_script(script_text)

    print(
        f"[worker-lua-key] stream={args.stream} group={args.group} consumer={args.consumer} field={args.field}"
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
                    # Fallback to entry_id when field missing
                    keyval = entry_id

                # If already processed by key, skip work but ack
                if r.sismember(processed_key, keyval):
                    ack_mark_key(keys=[processed_key], args=[args.stream, args.group, entry_id, keyval])
                    print(f"[skip-duplicate-key] id={entry_id} {args.field}={keyval}")
                    continue

                print(f"[work] id={entry_id} {args.field}={keyval} fields={fields}")
                try:
                    process(fields, delay=args.sleep)
                    res = ack_mark_key(keys=[processed_key], args=[args.stream, args.group, entry_id, keyval])
                    print(f"[ack+mark-key] id={entry_id} res={int(res)}")
                except Exception as e:
                    print(f"[error] id={entry_id} err={e}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)

