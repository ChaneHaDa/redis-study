from __future__ import annotations

import argparse
import signal
import sys
import time
from typing import List

from _common import get_redis_client


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Redis Pub/Sub subscriber")
    p.add_argument("channels", nargs="+", help="Channels to SUBSCRIBE (space-separated)")
    p.add_argument("--url", help="Redis URL, overrides REDIS_URL/env", default=None)
    p.add_argument("--poll", type=float, default=0.2, help="Polling interval seconds (default: 0.2)")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    r = get_redis_client(args.url)

    pubsub = r.pubsub(ignore_subscribe_messages=True)
    pubsub.subscribe(*args.channels)
    print(f"[subscriber] Subscribed to: {', '.join(args.channels)}")
    print("[subscriber] Press Ctrl+C to exit.")

    stop = False

    def _sigint(_signo, _frame):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, _sigint)
    signal.signal(signal.SIGTERM, _sigint)

    try:
        while not stop:
            msg = pubsub.get_message(timeout=1.0)
            if msg:
                # msg: {'type': 'message', 'pattern': None, 'channel': 'ch', 'data': 'payload'}
                ch = msg.get("channel")
                data = msg.get("data")
                print(f"[message] channel={ch} data={data}")
            else:
                # idle wait to avoid busy loop
                time.sleep(args.poll)
    finally:
        try:
            pubsub.close()
        except Exception:
            pass
        print("[subscriber] Closed.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)

