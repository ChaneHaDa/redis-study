import os
import time
import random
from multiprocessing import Pool
import redis

def get_client():
    url = os.getenv("REDIS_URL")
    return redis.Redis.from_url(url, decode_responses=True) if url \
        else redis.Redis(host="127.0.0.1", port=6379, db=0, decode_responses=True)

KEY = "coupon:count:race:proc"

def worker_incr(n):
    r = get_client()
    for _ in range(n):
        val = r.get(KEY)
        cur = int(val) if val is not None else 0
        time.sleep(random.uniform(0.0005, 0.005))
        r.set(KEY, cur + 1)

def run_proc_race(procs=8, ops_per_proc=200):
    r = get_client()
    r.delete(KEY)
    with Pool(processes=procs) as p:
        p.map(worker_incr, [ops_per_proc] * procs)

    time.sleep(0.5)
    final_val = int(r.get(KEY) or 0)
    expected = procs * ops_per_proc
    print(f"[PROC] expected={expected}, actual={final_val}, lost={expected - final_val}")

if __name__ == "__main__":
    run_proc_race()
