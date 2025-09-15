from __future__ import annotations

import argparse
import signal
import sys
import time

from _common import get_redis_client


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Redis keyevent notifications subscriber")
    p.add_argument("--db", type=int, default=0, help="DB index for keyevent channel (default: 0)")
    p.add_argument("--url", help="Redis URL, overrides REDIS_URL/env", default=None)
    p.add_argument("--poll", type=float, default=0.2, help="Polling interval seconds (default: 0.2)")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    r = get_redis_client(args.url)

    # Keyevent channel pattern for db N: __keyevent@N__:*  (e.g., expired, set, del, ...)
    pattern = f"__keyevent@{args.db}__:*"

    pubsub = r.pubsub(ignore_subscribe_messages=True)
    pubsub.psubscribe(pattern)
    print("[keysub] Ensure notify-keyspace-events enables 'E' (keyevent). Example:")
    print("[keysub]   redis-cli CONFIG SET notify-keyspace-events Ex")
    print(f"[keysub] PSubscribed to: {pattern}")
    print("[keysub] Press Ctrl+C to exit.")

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
                # e.g., expired event -> data holds the key name
                ev = msg.get("channel")
                key = msg.get("data")
                print(f"[keyevent] event={ev} key={key}")
            else:
                time.sleep(args.poll)
    finally:
        try:
            pubsub.close()
        except Exception:
            pass
        print("[keysub] Closed.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)

