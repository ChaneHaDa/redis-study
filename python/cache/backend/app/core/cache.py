"""
Redis 캐시 매니저
"""
from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from datetime import datetime, timezone
from typing import Optional, Any, Dict, Callable, Awaitable
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
        return max(1, base_ttl + jitter)  # 최소 1초 보장 (스탬피드 테스트용)
    
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
    
    async def set_with_soft_ttl(self, key: str, value: Any, ttl: int = None) -> bool:
        """Soft TTL과 함께 캐시에 데이터 저장 - 메타데이터 포함"""
        if not self.redis:
            return False
        
        try:
            ttl_with_jitter = self._get_ttl_with_jitter(ttl)
            
            # Soft TTL을 위한 메타데이터 추가
            # soft_expiry: 실제 만료 전 75% 지점에서 갱신 시작
            soft_ttl = int(ttl_with_jitter * 0.75)
            soft_expiry = datetime.now(timezone.utc).timestamp() + soft_ttl
            
            wrapped_value = {
                "data": value,
                "soft_expiry": soft_expiry,
                "created_at": datetime.now(timezone.utc).timestamp()
            }
            
            serialized_value = json.dumps(wrapped_value, ensure_ascii=False, default=str)
            
            result = await self.redis.setex(
                key, 
                ttl_with_jitter, 
                serialized_value
            )
            
            if result:
                self.metrics["sets"] += 1
                logger.debug(f"Cache SET with Soft TTL: {key} (Hard TTL: {ttl_with_jitter}s, Soft TTL: {soft_ttl}s)")
                return True
            return False
        except Exception as e:
            self.metrics["errors"] += 1
            logger.error(f"Cache set with soft TTL error for key {key}: {e}")
            return False
    
    async def get_with_soft_ttl_check(self, key: str) -> tuple[Optional[Any], bool]:
        """Soft TTL 체크와 함께 캐시에서 데이터 조회
        
        Returns:
            (data, needs_refresh): 데이터와 갱신 필요 여부
        """
        if not self.redis:
            return None, True
        
        try:
            value = await self.redis.get(key)
            if value is None:
                self.metrics["misses"] += 1
                logger.debug(f"Cache MISS: {key}")
                return None, True
            
            # 래핑된 데이터 파싱
            try:
                wrapped_data = json.loads(value)
                if isinstance(wrapped_data, dict) and "data" in wrapped_data:
                    # Soft TTL 메타데이터가 있는 경우
                    data = wrapped_data["data"]
                    soft_expiry = wrapped_data.get("soft_expiry", 0)
                    current_time = datetime.now(timezone.utc).timestamp()
                    
                    # Soft TTL 만료 확인
                    needs_refresh = current_time > soft_expiry
                    
                    self.metrics["hits"] += 1
                    if needs_refresh:
                        logger.debug(f"Cache HIT but needs refresh (Soft TTL expired): {key}")
                    else:
                        logger.debug(f"Cache HIT: {key}")
                    
                    return data, needs_refresh
                else:
                    # 일반 데이터 (레거시)
                    self.metrics["hits"] += 1
                    logger.debug(f"Cache HIT (legacy format): {key}")
                    return wrapped_data, False
                    
            except json.JSONDecodeError:
                # JSON 파싱 실패
                self.metrics["errors"] += 1
                logger.error(f"Cache data parsing error for key {key}")
                return None, True
                
        except Exception as e:
            self.metrics["errors"] += 1
            logger.error(f"Cache get with soft TTL error for key {key}: {e}")
            return None, True
    
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
    
    async def get_with_singleflight(self, 
                                   key: str, 
                                   fallback: Callable[[], Awaitable[Optional[Any]]], 
                                   ttl: int = None) -> Optional[Any]:
        """
        캐시 스탬피드 방지를 위한 Singleflight 패턴 구현
        Redis 락을 사용하여 하나의 프로세스만 데이터를 재계산하도록 보장
        """
        if not self.redis:
            return await fallback()
        
        # 1. 먼저 캐시에서 조회 시도
        cached_value = await self.get(key)
        if cached_value is not None:
            return cached_value
        
        # 2. 캐시 미스 - 락 키로 중복 계산 방지
        lock_key = f"lock:{key}"
        lock_ttl = 3000  # 3초 (밀리초)
        
        try:
            # 3. Redis SET NX PX 명령으로 락 획득 시도
            acquired = await self.redis.set(lock_key, "1", nx=True, px=lock_ttl)
            
            if acquired:
                # 락을 획득한 프로세스 - 데이터 재계산 및 캐시 저장
                logger.debug(f"Lock acquired for key: {key}")
                try:
                    # 락 획득 후 다시 한 번 캐시 확인 (다른 프로세스가 이미 저장했을 수 있음)
                    cached_value = await self.get(key)
                    if cached_value is not None:
                        return cached_value
                    
                    # 실제 데이터 재계산
                    value = await fallback()
                    if value is not None:
                        # 캐시에 저장
                        await self.set(key, value, ttl)
                    return value
                    
                finally:
                    # 락 해제
                    await self.redis.delete(lock_key)
                    logger.debug(f"Lock released for key: {key}")
            else:
                # 락을 획득하지 못한 프로세스들 - 짧게 대기 후 재시도
                logger.debug(f"Lock not acquired for key: {key}, waiting...")
                
                # 백오프 전략: 50ms ~ 200ms 랜덤 대기
                max_retries = 10
                base_delay = 0.05  # 50ms
                
                for retry in range(max_retries):
                    # 지수 백오프 + 지터
                    delay = base_delay * (2 ** min(retry, 3)) + random.uniform(0, 0.05)
                    await asyncio.sleep(delay)
                    
                    # 캐시에서 다시 조회 시도
                    cached_value = await self.get(key)
                    if cached_value is not None:
                        logger.debug(f"Got value from cache after waiting (retry {retry + 1}): {key}")
                        return cached_value
                    
                    # 락이 해제되었는지 확인
                    lock_exists = await self.redis.exists(lock_key)
                    if not lock_exists:
                        # 락이 해제되었지만 여전히 캐시에 값이 없다면 직접 계산
                        logger.debug(f"Lock released but no cache value, fallback to direct computation: {key}")
                        return await fallback()
                
                # 최대 재시도 횟수 초과 - fallback으로 직접 계산
                logger.warning(f"Max retries exceeded for key: {key}, falling back to direct computation")
                return await fallback()
                
        except Exception as e:
            logger.error(f"Singleflight error for key {key}: {e}")
            # 에러 발생 시 fallback으로 직접 계산
            return await fallback()
    
    async def get_with_soft_ttl_and_singleflight(self, 
                                                key: str, 
                                                fallback: Callable[[], Awaitable[Optional[Any]]], 
                                                ttl: int = None,
                                                refresh_probability: float = 0.1) -> Optional[Any]:
        """
        Soft TTL + Singleflight 조합 패턴
        
        1. 캐시 조회
        2. Soft TTL 만료 확인 
        3. 확률적 백그라운드 갱신 또는 즉시 갱신
        4. Singleflight로 중복 갱신 방지
        """
        if not self.redis:
            return await fallback()
        
        # 1. Soft TTL 체크와 함께 캐시 조회
        cached_data, needs_refresh = await self.get_with_soft_ttl_check(key)
        
        if cached_data is not None and not needs_refresh:
            # 캐시 히트, 갱신 불필요
            return cached_data
        
        if cached_data is not None and needs_refresh:
            # Soft TTL 만료 - 확률적 백그라운드 갱신
            should_refresh = random.random() < refresh_probability
            
            if should_refresh:
                # 선택된 요청이 백그라운드 갱신 수행
                logger.debug(f"Soft TTL expired, triggering background refresh for: {key}")
                asyncio.create_task(self._background_refresh(key, fallback, ttl))
            else:
                logger.debug(f"Soft TTL expired, but skipping refresh (probability): {key}")
            
            # 기존 데이터를 즉시 반환 (stale-while-revalidate)
            return cached_data
        
        # 캐시 미스 - Singleflight로 처리
        logger.debug(f"Cache miss, using singleflight for: {key}")
        value = await self._singleflight_fetch_and_store(key, fallback, ttl)
        return value
    
    async def _background_refresh(self, 
                                 key: str, 
                                 fallback: Callable[[], Awaitable[Optional[Any]]], 
                                 ttl: int = None):
        """백그라운드에서 캐시 갱신"""
        try:
            logger.debug(f"Background refresh starting for: {key}")
            value = await fallback()
            if value is not None:
                await self.set_with_soft_ttl(key, value, ttl)
                logger.debug(f"Background refresh completed for: {key}")
        except Exception as e:
            logger.error(f"Background refresh failed for key {key}: {e}")
    
    async def _singleflight_fetch_and_store(self, 
                                           key: str, 
                                           fallback: Callable[[], Awaitable[Optional[Any]]], 
                                           ttl: int = None) -> Optional[Any]:
        """Singleflight 패턴으로 데이터 가져와서 Soft TTL로 저장"""
        lock_key = f"lock:{key}"
        lock_ttl = 3000  # 3초 (밀리초)
        
        try:
            # Redis SET NX PX 명령으로 락 획득 시도
            acquired = await self.redis.set(lock_key, "1", nx=True, px=lock_ttl)
            
            if acquired:
                # 락을 획득한 프로세스
                logger.debug(f"Lock acquired for key: {key}")
                try:
                    # 락 획득 후 다시 한 번 캐시 확인
                    cached_data, _ = await self.get_with_soft_ttl_check(key)
                    if cached_data is not None:
                        return cached_data
                    
                    # 실제 데이터 재계산
                    value = await fallback()
                    if value is not None:
                        # Soft TTL로 캐시에 저장
                        await self.set_with_soft_ttl(key, value, ttl)
                    return value
                    
                finally:
                    # 락 해제
                    await self.redis.delete(lock_key)
                    logger.debug(f"Lock released for key: {key}")
            else:
                # 락을 획득하지 못한 프로세스들 - 백오프 대기
                logger.debug(f"Lock not acquired for key: {key}, waiting...")
                
                max_retries = 10
                base_delay = 0.05  # 50ms
                
                for retry in range(max_retries):
                    delay = base_delay * (2 ** min(retry, 3)) + random.uniform(0, 0.05)
                    await asyncio.sleep(delay)
                    
                    # 캐시에서 다시 조회 시도
                    cached_data, _ = await self.get_with_soft_ttl_check(key)
                    if cached_data is not None:
                        logger.debug(f"Got value from cache after waiting (retry {retry + 1}): {key}")
                        return cached_data
                    
                    # 락이 해제되었는지 확인
                    lock_exists = await self.redis.exists(lock_key)
                    if not lock_exists:
                        logger.debug(f"Lock released but no cache value, fallback: {key}")
                        return await fallback()
                
                # 최대 재시도 횟수 초과
                logger.warning(f"Max retries exceeded for key: {key}, falling back")
                return await fallback()
                
        except Exception as e:
            logger.error(f"Singleflight with soft TTL error for key {key}: {e}")
            return await fallback()


# 전역 캐시 매니저 인스턴스
cache_manager = CacheManager()