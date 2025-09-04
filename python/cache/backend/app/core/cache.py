"""
Redis 캐시 매니저
"""
from __future__ import annotations

import json
import logging
import random
from typing import Optional, Any, Dict
import redis.asyncio as aioredis
from redis.asyncio import Redis

from .config import settings

logger = logging.getLogger(__name__)


class CacheManager:
    """Redis 캐시 매니저 - Cache-Aside 패턴 구현"""
    
    def __init__(self):
        self.redis: Optional[Redis] = None
        self.metrics: Dict[str, int] = {
            "hits": 0,
            "misses": 0,
            "sets": 0,
            "errors": 0
        }
    
    async def connect(self):
        """Redis 연결 생성"""
        try:
            self.redis = aioredis.from_url(
                settings.REDIS_URL,
                encoding="utf-8",
                decode_responses=True,
                socket_connect_timeout=5,
                socket_keepalive=True,
                health_check_interval=30
            )
            # 연결 테스트
            await self.redis.ping()
            logger.info(f"Redis 연결 성공: {settings.REDIS_URL}")
        except Exception as e:
            logger.error(f"Redis 연결 실패: {e}")
            self.redis = None
    
    async def disconnect(self):
        """Redis 연결 해제"""
        if self.redis:
            await self.redis.aclose()
            self.redis = None
            logger.info("Redis 연결 해제")
    
    def _get_ttl_with_jitter(self, base_ttl: int = None) -> int:
        """TTL에 지터를 적용하여 동시 만료 방지"""
        if base_ttl is None:
            base_ttl = settings.CACHE_TTL
        
        jitter_range = base_ttl * settings.CACHE_JITTER_PERCENT // 100
        jitter = random.randint(-jitter_range, jitter_range)
        return max(60, base_ttl + jitter)  # 최소 60초 보장
    
    async def get(self, key: str) -> Optional[Any]:
        """캐시에서 데이터 조회"""
        if not self.redis:
            return None
        
        try:
            value = await self.redis.get(key)
            if value is not None:
                self.metrics["hits"] += 1
                logger.debug(f"Cache HIT: {key}")
                return json.loads(value)
            else:
                self.metrics["misses"] += 1
                logger.debug(f"Cache MISS: {key}")
                return None
        except Exception as e:
            self.metrics["errors"] += 1
            logger.error(f"Cache get error for key {key}: {e}")
            return None
    
    async def set(self, key: str, value: Any, ttl: int = None) -> bool:
        """캐시에 데이터 저장"""
        if not self.redis:
            return False
        
        try:
            ttl_with_jitter = self._get_ttl_with_jitter(ttl)
            serialized_value = json.dumps(value, ensure_ascii=False, default=str)
            
            result = await self.redis.setex(
                key, 
                ttl_with_jitter, 
                serialized_value
            )
            
            if result:
                self.metrics["sets"] += 1
                logger.debug(f"Cache SET: {key} (TTL: {ttl_with_jitter}s)")
                return True
            return False
        except Exception as e:
            self.metrics["errors"] += 1
            logger.error(f"Cache set error for key {key}: {e}")
            return False
    
    async def delete(self, key: str) -> bool:
        """캐시에서 데이터 삭제"""
        if not self.redis:
            return False
        
        try:
            result = await self.redis.delete(key)
            if result > 0:
                logger.debug(f"Cache DELETE: {key}")
            return result > 0
        except Exception as e:
            self.metrics["errors"] += 1
            logger.error(f"Cache delete error for key {key}: {e}")
            return False
    
    async def clear_all(self) -> bool:
        """모든 캐시 데이터 삭제"""
        if not self.redis:
            return False
        
        try:
            await self.redis.flushdb()
            logger.info("All cache data cleared")
            return True
        except Exception as e:
            self.metrics["errors"] += 1
            logger.error(f"Cache clear error: {e}")
            return False
    
    def get_metrics(self) -> Dict[str, Any]:
        """캐시 메트릭 반환"""
        total_requests = self.metrics["hits"] + self.metrics["misses"]
        hit_rate = (self.metrics["hits"] / total_requests * 100) if total_requests > 0 else 0
        
        return {
            **self.metrics,
            "total_requests": total_requests,
            "hit_rate_percent": round(hit_rate, 2)
        }
    
    def reset_metrics(self):
        """메트릭 초기화"""
        self.metrics = {
            "hits": 0,
            "misses": 0,
            "sets": 0,
            "errors": 0
        }
        logger.info("Cache metrics reset")


# 전역 캐시 매니저 인스턴스
cache_manager = CacheManager()