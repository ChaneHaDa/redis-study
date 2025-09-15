from __future__ import annotations

import argparse
import sys
import time

from dist_lock import DistLock


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Acquire lock and auto-renew (watchdog)")
    p.add_argument("--resource", required=True, help="Resource name")
    p.add_argument("--ttl-ms", type=int, default=5000, help="Lock TTL in ms (default: 5000)")
    p.add_argument("--renew-ms", type=int, default=2000, help="Renew interval ms (default: 2000)")
    p.add_argument("--work-ms", type=int, default=15000, help="Simulated work time in ms (default: 15000)")
    p.add_argument("--url", default=None, help="Redis URL")
    return p.parse_args()


def main() -> None:
    a = parse_args()
    lock = DistLock(resource=a.resource, ttl_ms=a.ttl_ms, url=a.url)
    if not lock.acquire(timeout_ms=2000, retry_ms=100):
        print("[lock] acquire failed")
        sys.exit(1)
    print(f"[lock] acquired key=lock:{a.resource} owner={lock.owner} ttl_ms={a.ttl_ms}")
    lock.start_renew(every_ms=a.renew_ms)
    try:
        time.sleep(a.work_ms / 1000.0)
    finally:
        released = lock.release()
        print(f"[lock] released={released}")


if __name__ == "__main__":
    main()

