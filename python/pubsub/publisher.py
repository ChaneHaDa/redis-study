from __future__ import annotations

import argparse
import sys
import time

from _common import get_redis_client


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Redis Pub/Sub publisher")
    p.add_argument("--channel", required=True, help="Channel to PUBLISH")
    p.add_argument("--message", required=True, help="Message payload (string)")
    p.add_argument("--count", type=int, default=1, help="Times to publish (default: 1)")
    p.add_argument("--interval", type=float, default=0.0, help="Delay seconds between publishes")
    p.add_argument("--url", help="Redis URL, overrides REDIS_URL/env", default=None)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    r = get_redis_client(args.url)

    for i in range(1, args.count + 1):
        receivers = r.publish(args.channel, args.message)
        print(f"[publish] #{i} channel={args.channel} receivers={receivers} message={args.message}")
        if i < args.count and args.interval > 0:
            time.sleep(args.interval)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)

