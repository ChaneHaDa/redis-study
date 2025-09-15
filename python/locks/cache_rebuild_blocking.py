from __future__ import annotations

import argparse
import sys
import time

from _common import get_redis_client
from dist_lock import DistLock


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Cache-aside rebuild with distributed lock (blocking)")
    p.add_argument("--key", default="item:1", help="Logical cache key (e.g., item:1)")
    p.add_argument("--cache-ttl", type=int, default=30, help="Cache TTL seconds (default: 30)")
    p.add_argument("--lock-ttl-ms", type=int, default=5000, help="Lock TTL ms (default: 5000)")
    p.add_argument("--timeout-ms", type=int, default=1000, help="Lock wait timeout ms (default: 1000)")
    p.add_argument("--retry-ms", type=int, default=100, help="Retry backoff ms (default: 100)")
    p.add_argument("--wait-fill-ms", type=int, default=1500, help="Wait for cache fill ms if lock not acquired (default: 1500)")
    p.add_argument("--db-ms", type=int, default=500, help="Simulated DB latency ms (default: 500)")
    p.add_argument("--watchdog", action="store_true", help="Auto-renew lock while rebuilding")
    p.add_argument("--url", default=None, help="Redis URL")
    return p.parse_args()


def cache_key(key: str) -> str:
    return f"cache:{key}"


def lock_resource(key: str) -> str:
    return f"rebuild:{key}"


def simulate_db_fetch(key: str, db_ms: int) -> str:
    # Simulate DB call and return new value
    time.sleep(db_ms / 1000.0)
    return f"db-value@{int(time.time())}"


def main() -> None:
    a = parse_args()
    r = get_redis_client(a.url)
    ckey = cache_key(a.key)

    val = r.get(ckey)
    if val is not None:
        print(f"[cache] HIT key={ckey} val={val}")
        return

    print(f"[cache] MISS key={ckey} → try lock and rebuild")
    lock = DistLock(resource=lock_resource(a.key), ttl_ms=a.lock_ttl_ms, url=a.url)
    acquired = lock.acquire(timeout_ms=a.timeout_ms, retry_ms=a.retry_ms)
    if acquired:
        print(f"[lock] acquired by {lock.owner}")
        try:
            if a.watchdog:
                lock.start_renew(every_ms=max(1000, a.lock_ttl_ms // 2))
            new_val = simulate_db_fetch(a.key, a.db_ms)
            r.setex(ckey, a.cache_ttl, new_val)
            print(f"[cache] SETEX key={ckey} ttl={a.cache_ttl}s val={new_val}")
            print("[return] fresh value")
        finally:
            released = lock.release()
            print(f"[lock] released={released}")
    else:
        print("[lock] not acquired → waiting for fill by another worker")
        deadline = time.time() + (a.wait_fill_ms / 1000.0)
        while time.time() < deadline:
            val = r.get(ckey)
            if val is not None:
                print(f"[cache] FILLED key={ckey} val={val}")
                print("[return] filled value")
                return
            time.sleep(0.05)
        print("[timeout] cache not filled in time")
        sys.exit(2)


if __name__ == "__main__":
    main()

