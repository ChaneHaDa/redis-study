import os
import time
from typing import Any

import redis

def get_redis_client() -> redis.Redis:
    """Redis 클라이언트를 생성해 반환합니다.
    - 환경변수 REDIS_URL이 있으면 그 값을 사용합니다.
    - 없으면 로컬 127.0.0.1:6379, DB 0에 접속합니다.
    - decode_responses=True로 지정해 str 타입으로 응답을 받습니다.
    """
    url = os.getenv("REDIS_URL")
    if url:
        return redis.Redis.from_url(url, decode_responses=True)
    # 로컬 기본값
    return redis.Redis(host="127.0.0.1", port=6379, db=0, decode_responses=True)

def main() -> None:
    """실제 Redis에 연결해서 기본 명령들을 순차적으로 실행하는 데모."""
    r = get_redis_client()
    # 연결 확인 (PING)
    print("PING ->", r.ping())

    print("\n== Naive coupon feel (not safe for race) ==")
    # 단순한 쿠폰 카운터 예제 (실제로는 WATCH/MULTI나 Lua로 레이스 컨디션 방지 필요 -> 쿠폰을 확인하고 발급하는 경우!)
    for _ in range(5):
        r.incr("coupon:count")
    print("coupon:count ->", r.get("coupon:count"))

    print("\nDone.")

if __name__ == "__main__":
    main()