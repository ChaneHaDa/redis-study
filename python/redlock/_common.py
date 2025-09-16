from __future__ import annotations

import os
from typing import List

import redis


def def get_redis_clients(urls: str | None = None) -> List[redis.Redis]:
    urls_str = urls or os.getenv("REDIS_URLS")
    if not urls_str:
        raise ValueError("Redis URLs not provided. Set REDIS_URLS env var or pass --urls.")

    clients = []
    for url in urls_str.split(','):
        clients.append(redis.Redis.from_url(url.strip(), decode_responses=True))
    return clients
