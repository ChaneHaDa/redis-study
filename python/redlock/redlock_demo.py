from __future__ import annotations

import argparse
import sys
import time

from _common import get_redis_clients
from redlock import Redlock


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Redlock demo")
    p.add_argument("--resource", required=True, help="Resource name")
    p.add_argument("--ttl-ms", type=int, default=10000, help="Lock TTL in ms")
    p.add_argument("--work-ms", type=int, default=4000, help="Work time ms")
    p.add_argument(
        "--urls",
        default=None,
        help="Comma-separated Redis URLs (e.g., redis://localhost:6380,redis://localhost:6381)",
    )
    return p.parse_args()


def main() -> None:
    a = parse_args()
    try:
        masters = get_redis_clients(a.urls)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"Attempting to acquire lock for resource '{a.resource}' on {len(masters)} masters...")

    try:
        with Redlock(masters=masters, resource=a.resource, ttl_ms=a.ttl_ms) as lock:
            print(f"[lock] Acquired! owner={lock.owner}, quorum={lock.quorum}")
            print(f"  > Performing work for {a.work_ms}ms...")
            time.sleep(a.work_ms / 1000.0)
            print("  > Work finished.")
        print("[lock] Released via context manager.")
    except RuntimeError as e:
        print(f"[lock] Failed: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
