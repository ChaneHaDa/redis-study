from __future__ import annotations

import argparse
import random
import sys
import time
from typing import Dict, List

from _common import get_redis_client


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Redis Streams consumer group worker (XREADGROUP + XACK)")
    p.add_argument("--stream", required=True, help="Stream key")
    p.add_argument("--group", required=True, help="Group name")
    p.add_argument("--consumer", required=True, help="Consumer name (unique per worker)")
    p.add_argument("--count", type=int, default=10, help="Max entries per read")
    p.add_argument("--block", type=int, default=5000, help="Block milliseconds (default: 5000)")
    p.add_argument("--noack", action="store_true", help="Do not XACK (simulate crash)")
    p.add_argument("--sleep", type=float, default=0.0, help="Seconds to sleep per entry (simulate work)")
    p.add_argument("--url", default=None, help="Redis URL")
    return p.parse_args()


def process(fields: Dict[str, str], delay: float = 0.0) -> None:
    # Placeholder for doing real work
    if delay > 0:
        time.sleep(delay)


def main() -> None:
    args = parse_args()
    r = get_redis_client(args.url)

    print(f"[worker] stream={args.stream} group={args.group} consumer={args.consumer}")
    while True:
        data = r.xreadgroup(groupname=args.group, consumername=args.consumer,
                             streams={args.stream: ">"}, count=args.count, block=args.block)
        if not data:
            print("[xreadgroup] timeout (no entries)")
            continue
        # data: List[Tuple[stream, List[Tuple[id, fields]]]]
        for stream_key, entries in data:
            for entry_id, fields in entries:
                print(f"[work] id={entry_id} fields={fields}")
                try:
                    process(fields, delay=args.sleep)
                    if not args.noack:
                        r.xack(stream_key, args.group, entry_id)
                        print(f"[ack] id={entry_id}")
                    else:
                        print(f"[skip-ack] id={entry_id}")
                except Exception as e:
                    print(f"[error] id={entry_id} err={e}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)

