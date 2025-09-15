from __future__ import annotations

import os
from typing import Optional

import redis


def get_redis_client(url: Optional[str] = None) -> redis.Redis:
    if url:
        return redis.Redis.from_url(url, decode_responses=True)
    env = os.getenv("REDIS_URL")
    if env:
        return redis.Redis.from_url(env, decode_responses=True)
    return redis.Redis(host="127.0.0.1", port=6379, db=0, decode_responses=True)

