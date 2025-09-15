from __future__ import annotations

import argparse
import signal
import sys
import time

from _common import get_redis_client


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Redis Pub/Sub pattern subscriber")
    p.add_argument("patterns", nargs="+", help="Patterns to PSUBSCRIBE (e.g., 'chat:*')")
    p.add_argument("--url", help="Redis URL, overrides REDIS_URL/env", default=None)
    p.add_argument("--poll", type=float, default=0.2, help="Polling interval seconds (default: 0.2)")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    r = get_redis_client(args.url)

    pubsub = r.pubsub(ignore_subscribe_messages=True)
    pubsub.psubscribe(*args.patterns)
    print(f"[psubscriber] PSubscribed to: {', '.join(args.patterns)}")
    print("[psubscriber] Press Ctrl+C to exit.")

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
                # msg: {'type': 'pmessage', 'pattern': 'chat:*', 'channel': 'chat:room1', 'data': '...'}
                pat = msg.get("pattern")
                ch = msg.get("channel")
                data = msg.get("data")
                print(f"[pmessage] pattern={pat} channel={ch} data={data}")
            else:
                time.sleep(args.poll)
    finally:
        try:
            pubsub.close()
        except Exception:
            pass
        print("[psubscriber] Closed.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)

