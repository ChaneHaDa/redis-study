from __future__ import annotations

import argparse
import sys
import time
import uuid

from dist_lock import DistLock


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Acquire lock once, do work, release")
    p.add_argument("--resource", required=True, help="Resource name")
    p.add_argument("--ttl-ms", type=int, default=5000, help="Lock TTL in ms (default: 5000)")
    p.add_argument("--work-ms", type=int, default=2000, help="Simulated work time in ms (default: 2000)")
    p.add_argument("--owner", default=None, help="Owner token (default: random UUID)")
    p.add_argument("--url", default=None, help="Redis URL")
    return p.parse_args()


def main() -> None:
    a = parse_args()
    lock = DistLock(resource=a.resource, ttl_ms=a.ttl_ms, owner=a.owner, url=a.url)
    if not lock.try_acquire():
        print("[lock] acquire failed (already held)")
        sys.exit(1)
    print(f"[lock] acquired key=lock:{a.resource} owner={lock.owner} ttl_ms={a.ttl_ms}")
    try:
        time.sleep(a.work_ms / 1000.0)
    finally:
        released = lock.release()
        print(f"[lock] released={released}")


if __name__ == "__main__":
    main()

