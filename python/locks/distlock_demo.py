from __future__ import annotations

import argparse
import sys
import time

from dist_lock import DistLock


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="DistLock demo with context manager")
    p.add_argument("--resource", required=True, help="Resource name")
    p.add_argument("--ttl-ms", type=int, default=5000, help="Lock TTL in ms")
    p.add_argument("--work-ms", type=int, default=3000, help="Work time ms")
    p.add_argument("--url", default=None, help="Redis URL")
    return p.parse_args()


def main() -> None:
    a = parse_args()
    lock = DistLock(resource=a.resource, ttl_ms=a.ttl_ms, url=a.url)
    if not lock.acquire(timeout_ms=2000, retry_ms=100):
        print("[lock] acquire failed")
        sys.exit(1)
    with lock:
        print(f"[lock] in critical section owner={lock.owner}")
        time.sleep(a.work_ms / 1000.0)
    print("[lock] released via context manager")


if __name__ == "__main__":
    main()

