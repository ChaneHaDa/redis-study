from __future__ import annotations

import argparse
import json
import sys
import time

from _common import get_redis_client
from dist_lock import DistLock


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Cache-aside with Stale-While-Revalidate (SWR) + distributed lock")
    p.add_argument("--key", default="item:1", help="Logical cache key (e.g., item:1)")
    p.add_argument("--soft-ttl", type=int, default=10, help="Freshness window seconds (soft TTL, default: 10)")
    p.add_argument("--swr-window", type=int, default=10, help="Serve-stale window seconds beyond soft TTL (default: 10)")
    p.add_argument("--lock-ttl-ms", type=int, default=5000, help="Lock TTL ms (default: 5000)")
    p.add_argument("--db-ms", type=int, default=500, help="Simulated DB latency ms (default: 500)")
    p.add_argument("--url", default=None, help="Redis URL")
    return p.parse_args()


def cache_key(key: str) -> str:
    return f"cache:{key}"


def lock_resource(key: str) -> str:
    return f"rebuild:{key}"


def simulate_db_fetch(key: str, db_ms: int) -> str:
    time.sleep(db_ms / 1000.0)
    return f"db-value@{int(time.time())}"


def encode_value(data: str, soft_expire_at_ms: int) -> str:
    return json.dumps({"val": data, "soft_expire_at": soft_expire_at_ms})


def decode_value(raw: str) -> tuple[str, int] | None:
    try:
        obj = json.loads(raw)
        return str(obj.get("val")), int(obj.get("soft_expire_at"))
    except Exception:
        return None


def main() -> None:
    a = parse_args()
    r = get_redis_client(a.url)
    ckey = cache_key(a.key)

    now_ms = int(time.time() * 1000)
    raw = r.get(ckey)
    if raw is None:
        print(f"[cache] COLD MISS key={ckey} → rebuild")
        lock = DistLock(resource=lock_resource(a.key), ttl_ms=a.lock_ttl_ms, url=a.url)
        if lock.acquire(timeout_ms=1000, retry_ms=100):
            try:
                new_val = simulate_db_fetch(a.key, a.db_ms)
                soft_exp = now_ms + (a.soft_ttl * 1000)
                hard_ttl = a.soft_ttl + a.swr_window
                r.setex(ckey, hard_ttl, encode_value(new_val, soft_exp))
                print(f"[cache] SETEX soft={a.soft_ttl}s hard={hard_ttl}s val={new_val}")
                print("[return] fresh value")
                return
            finally:
                lock.release()
        else:
            print("[lock] not acquired. Try shortly again.")
            sys.exit(2)

    decoded = decode_value(raw)
    if not decoded:
        print("[cache] invalid payload. Evict and retry.")
        r.delete(ckey)
        sys.exit(3)

    val, soft_expire_at = decoded
    if now_ms <= soft_expire_at:
        # fresh
        remain_ms = soft_expire_at - now_ms
        print(f"[cache] FRESH key={ckey} remain_soft={remain_ms}ms val={val}")
        return

    # stale but within SWR window
    ttl = r.ttl(ckey)
    if ttl is None or ttl <= 0:
        # hard expired between GET and now → treat as cold
        print("[cache] hard expired; treat as COLD MISS")
        r.delete(ckey)
        sys.exit(4)

    print(f"[cache] STALE (serve and revalidate) key={ckey} swr_ttl={ttl}s val={val}")
    # Try to refresh in-line if lock available, else just serve stale
    lock = DistLock(resource=lock_resource(a.key), ttl_ms=a.lock_ttl_ms, url=a.url)
    if lock.try_acquire():
        try:
            new_val = simulate_db_fetch(a.key, a.db_ms)
            soft_exp = int(time.time() * 1000) + (a.soft_ttl * 1000)
            hard_ttl = a.soft_ttl + a.swr_window
            r.setex(ckey, hard_ttl, encode_value(new_val, soft_exp))
            print(f"[refresh] SETEX soft={a.soft_ttl}s hard={hard_ttl}s val={new_val}")
        finally:
            lock.release()
    else:
        print("[refresh] another worker is rebuilding; served stale")


if __name__ == "__main__":
    main()

