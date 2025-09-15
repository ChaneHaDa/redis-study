from __future__ import annotations

import argparse
import sys
import time

from dist_lock import DistLock


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Blocking acquire with timeout/backoff")
    p.add_argument("--resource", required=True, help="Resource name")
    p.add_argument("--ttl-ms", type=int, default=5000, help="Lock TTL in ms (default: 5000)")
    p.add_argument("--timeout-ms", type=int, default=10000, help="Max wait ms (default: 10000)")
    p.add_argument("--retry-ms", type=int, default=200, help="Base retry backoff ms (default: 200)")
    p.add_argument("--work-ms", type=int, default=2000, help="Simulated work time in ms (default: 2000)")
    p.add_argument("--url", default=None, help="Redis URL")
    return p.parse_args()


def main() -> None:
    a = parse_args()
    lock = DistLock(resource=a.resource, ttl_ms=a.ttl_ms, url=a.url)
    ok = lock.acquire(timeout_ms=a.timeout_ms, retry_ms=a.retry_ms)
    if not ok:
        print("[lock] acquire timed out")
        sys.exit(1)
    print(f"[lock] acquired key=lock:{a.resource} owner={lock.owner}")
    try:
        time.sleep(a.work_ms / 1000.0)
    finally:
        released = lock.release()
        print(f"[lock] released={released}")


if __name__ == "__main__":
    main()

