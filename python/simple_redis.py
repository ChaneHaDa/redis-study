"""
실제 Redis 서버에 연결해서 basic 명령들을 실행하는 데모.

사전 준비:
- Redis 서버가 로컬 6379에서 실행 중이거나, 환경변수 REDIS_URL 제공
  예) REDIS_URL=redis://:password@localhost:6379/0

설치:
pip install redis

실행:
python real_redis_demo.py
"""

from __future__ import annotations
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

    print("\n== String + TTL ==")
    # ex=2 로 2초 뒤 만료되도록 설정
    r.set("greet", "hello", ex=2)
    print("GET greet ->", r.get("greet"))
    print("TTL greet ->", r.ttl("greet"))
    # 만료 확인을 위해 2초보다 약간 더 대기
    time.sleep(2.1)
    print("TTL greet (after sleep) ->", r.ttl("greet"))
    print("GET greet (after expire) ->", r.get("greet"))

    print("\n== INCR/DECR ==")
    # 실험을 위해 기존 키 제거
    r.delete("count")
    print("INCR count ->", r.incr("count"))
    print("INCR count ->", r.incr("count", 10))
    print("DECR count ->", r.decr("count"))

    print("\n== Hash ==")
    # 해시 키 초기화 후 필드 설정
    r.delete("user:42")
    print("HSET user:42 ->", r.hset("user:42", mapping={"name": "해찬", "role": "backend"}))
    print("HINCRBY user:42 posts ->", r.hincrby("user:42", "posts", 1))
    print("HGETALL user:42 ->", r.hgetall("user:42"))

    print("\n== List (recent 5) ==")
    # 최근 5개만 유지하는 리스트 패턴 (LPUSH + LTRIM)
    r.delete("recent:42")
    for i in range(1, 8):
        r.lpush("recent:42", f"post-{i}")
        r.ltrim("recent:42", 0, 4)  # 최근 5개 유지
    print("LRANGE recent:42 0 -1 ->", r.lrange("recent:42", 0, -1))

    print("\n== Set ==")
    # 집합은 중복을 허용하지 않음 (u2는 한 번만 저장)
    r.delete("viewed:99")
    print("SADD viewed:99 ->", r.sadd("viewed:99", "u1", "u2", "u2"))
    # 정렬해 출력하면 확인이 편리함 (Redis는 내부적으로 정렬 보장 안 함)
    print("SMEMBERS viewed:99 ->", sorted(r.smembers("viewed:99")))
    print("SISMEMBER viewed:99 u3 ->", r.sismember("viewed:99", "u3"))

    print("\n== Sorted Set (ranking) ==")
    # 점수(score)로 정렬되는 랭킹 예제
    r.delete("rank:game")
    print("ZADD rank:game ->", r.zadd("rank:game", {"alice": 30, "bob": 50}))
    print("ZINCRBY rank:game carol 15 ->", r.zincrby("rank:game", 15, "carol"))
    print("ZREVRANGE rank:game 0 2 WITHSCORES ->", r.zrevrange("rank:game", 0, 2, withscores=True))

    print("\nDone.")


if __name__ == "__main__":
    main()


