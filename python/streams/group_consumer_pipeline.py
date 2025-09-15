from __future__ import annotations

import argparse
import sys
import time
from typing import Dict

from _common import get_redis_client


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Consumer group worker with pipelined ACK + processed mark")
    p.add_argument("--stream", required=True, help="Stream key")
    p.add_argument("--group", required=True, help="Group name")
    p.add_argument("--consumer", required=True, help="Consumer name")
    p.add_argument("--count", type=int, default=20, help="Max entries per read")
    p.add_argument("--block", type=int, default=5000, help="Block milliseconds (default: 5000)")
    p.add_argument("--sleep", type=float, default=0.0, help="Simulated processing seconds per entry")
    p.add_argument("--batch", type=int, default=50, help="Pipeline batch size before execute")
    p.add_argument("--url", default=None, help="Redis URL")
    return p.parse_args()


def process(fields: Dict[str, str], delay: float = 0.0) -> None:
    if delay > 0:
        time.sleep(delay)


def main() -> None:
    args = parse_args()
    r = get_redis_client(args.url)
    processed_key = f"proc:{args.stream}"

    print(f"[worker-pipe] stream={args.stream} group={args.group} consumer={args.consumer}")

    pipe = r.pipeline(transaction=False)
    pending_ops = 0

    def flush():
        nonlocal pending_ops
        if pending_ops > 0:
            pipe.execute()
            pending_ops = 0

    try:
        while True:
            data = r.xreadgroup(groupname=args.group, consumername=args.consumer,
                                 streams={args.stream: ">"}, count=args.count, block=args.block)
            if not data:
                print("[xreadgroup] timeout (no entries)")
                flush()
                continue
            for stream_key, entries in data:
                for entry_id, fields in entries:
                    # Skip if already processed
                    if r.sismember(processed_key, entry_id):
                        pipe.xack(stream_key, args.group, entry_id)
                        pending_ops += 1
                        continue
                    # Do work then mark+ack via pipeline
                    print(f"[work] id={entry_id} fields={fields}")
                    try:
                        process(fields, delay=args.sleep)
                        pipe.sadd(processed_key, entry_id)
                        pipe.xack(stream_key, args.group, entry_id)
                        pending_ops += 2
                    except Exception as e:
                        print(f"[error] id={entry_id} err={e}")

                    if pending_ops >= args.batch:
                        flush()
    finally:
        flush()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)

