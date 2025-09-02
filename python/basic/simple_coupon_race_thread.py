import os
import time
import random
from concurrent.futures import ThreadPoolExecutor
import redis

def get_client():
    url = os.getenv("REDIS_URL")
    return redis.Redis.from_url(url, decode_responses=True) if url \
        else redis.Redis(host="127.0.0.1", port=6379, db=0, decode_responses=True)

KEY = "coupon:count:race:thread"

def naive_increment_once():
    r = get_client()  # 스레드마다 독립 연결 권장
    # 읽기 → (의도적 지연) → 쓰기: 원자성 보장 X
    val = r.get(KEY)
    current = int(val) if val is not None else 0
    # 레이스 창을 키우는 임의 지연
    time.sleep(random.uniform(0.001, 0.01))
    r.set(KEY, current + 1)

def run_thread_race(total_ops=500, workers=50):
    r = get_client()
    r.delete(KEY)
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for _ in range(total_ops):
            ex.submit(naive_increment_once)

    time.sleep(0.5)  # 마무리 대기
    final_val = int(r.get(KEY) or 0)
    print(f"[THREAD] expected={total_ops}, actual={final_val}, lost={total_ops - final_val}")

if __name__ == "__main__":
    run_thread_race()
