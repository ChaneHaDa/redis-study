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
from enum import Enum
from typing import Optional, Any, Dict, Callable, Awaitable
import redis.asyncio as aioredis
from redis.asyncio import Redis

from .config import settings

logger = logging.getLogger(__name__)


class WritePattern(Enum):
    """쓰기 패턴 모드"""
    INVALIDATION = "invalidation"    # DB 커밋 후 DEL
    WRITE_THROUGH = "write_through"  # DB 쓰기 후 바로 SET
    WRITE_BEHIND = "write_behind"    # 캐시 먼저, DB는 비동기


class CacheManager:
    """Redis 캐시 매니저 - Cache-Aside 패턴 구현"""
    
    def __init__(self):
        self.redis: Optional[Redis] = None
        self.metrics: Dict[str, int] = {
            "hits": 0,
            "misses": 0,
            "sets": 0,
            "errors": 0,
            "invalidations": 0,
            "write_through": 0,
            "write_behind": 0
        }
        self.write_pattern = WritePattern.INVALIDATION  # 기본값
    
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
                # 값 파싱 + Soft TTL 래핑 데이터 자동 언래핑
                try:
                    parsed = json.loads(value)
                    # Soft TTL로 저장된 경우 { data, soft_expiry, created_at } 형태
                    if isinstance(parsed, dict) and "data" in parsed and (
                        "soft_expiry" in parsed or "created_at" in parsed
                    ):
                        return parsed.get("data")
                    return parsed
                except json.JSONDecodeError:
                    # JSON이 아니면 원문 반환 (비정상 케이스)
                    logger.warning(f"Cache value for key {key} is not valid JSON; returning raw")
                    return value
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
            "errors": 0,
            "invalidations": 0,
            "write_through": 0,
            "write_behind": 0
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
    
    def set_write_pattern(self, pattern: WritePattern):
        """쓰기 패턴 모드 설정"""
        self.write_pattern = pattern
        logger.info(f"Write pattern changed to: {pattern.value}")
    
    def get_write_pattern(self) -> WritePattern:
        """현재 쓰기 패턴 모드 조회"""
        return self.write_pattern
    
    async def handle_write_invalidation(self, key: str) -> bool:
        """Invalidation 패턴: DB 커밋 후 캐시 삭제"""
        try:
            result = await self.delete(key)
            if result:
                self.metrics["invalidations"] += 1
                logger.debug(f"INVALIDATION: Deleted cache key {key}")
            return result
        except Exception as e:
            logger.error(f"Invalidation error for key {key}: {e}")
            return False
    
    async def handle_write_through(self, key: str, value: Any, ttl: int = None) -> bool:
        """Write-Through 패턴: DB 쓰기 성공 후 바로 캐시 업데이트"""
        try:
            result = await self.set_with_soft_ttl(key, value, ttl)
            if result:
                self.metrics["write_through"] += 1
                logger.debug(f"WRITE-THROUGH: Updated cache key {key}")
            return result
        except Exception as e:
            logger.error(f"Write-through error for key {key}: {e}")
            return False
    
    async def handle_write_behind(self, key: str, value: Any, ttl: int = None) -> bool:
        """Write-Behind 패턴: 먼저 캐시 업데이트, DB는 큐를 통해 비동기"""
        try:
            # 1. 캐시 먼저 업데이트
            cache_result = await self.set_with_soft_ttl(key, value, ttl)
            
            # 2. DB 변경사항을 큐에 추가
            if cache_result:
                await self._enqueue_db_write(key, value)
                self.metrics["write_behind"] += 1
                logger.debug(f"WRITE-BEHIND: Updated cache and enqueued DB write for {key}")
            
            return cache_result
        except Exception as e:
            logger.error(f"Write-behind error for key {key}: {e}")
            return False
    
    async def _enqueue_db_write(self, key: str, value: Any):
        """DB 쓰기 작업을 Redis Stream에 큐잉"""
        if not self.redis:
            return
        
        try:
            # Redis Stream을 사용한 큐잉
            stream_name = "db_write_queue"
            write_task = {
                "key": key,
                "value": json.dumps(value, ensure_ascii=False, default=str),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "action": "update"
            }
            
            await self.redis.xadd(stream_name, write_task)
            logger.debug(f"Enqueued DB write task for key: {key}")
            
        except Exception as e:
            logger.error(f"Failed to enqueue DB write for key {key}: {e}")
    
    async def process_write_behind_queue(self, batch_size: int = 10) -> int:
        """Write-Behind 큐 처리 - 배치로 DB 쓰기 작업 수행"""
        if not self.redis:
            return 0
        
        try:
            stream_name = "db_write_queue"
            consumer_group = "db_writers"
            consumer_name = f"worker_{int(time.time())}"
            
            # Consumer Group 생성 (이미 존재하면 무시)
            try:
                await self.redis.xgroup_create(stream_name, consumer_group, "0", mkstream=True)
            except:
                pass  # 이미 존재하는 경우 무시
            
            # 메시지 읽기
            messages = await self.redis.xreadgroup(
                consumer_group, 
                consumer_name, 
                {stream_name: ">"}, 
                count=batch_size
            )
            
            processed_count = 0
            for stream, stream_messages in messages:
                for message_id, fields in stream_messages:
                    try:
                        # DB 쓰기 작업 수행 (여기서는 로깅만)
                        key = fields.get('key')
                        value = fields.get('value')
                        logger.debug(f"Processing write-behind task: {key}")
                        
                        # 실제 DB 쓰기는 여기서 수행
                        await self._perform_db_write(key, value)
                        
                        # 메시지 ACK
                        await self.redis.xack(stream_name, consumer_group, message_id)
                        processed_count += 1
                        
                    except Exception as e:
                        logger.error(f"Failed to process write-behind task {message_id}: {e}")
            
            if processed_count > 0:
                logger.info(f"Processed {processed_count} write-behind tasks")
            
            return processed_count
            
        except Exception as e:
            logger.error(f"Error processing write-behind queue: {e}")
            return 0
    
    async def _perform_db_write(self, key: str, value_json: str):
        """실제 DB 쓰기 작업 수행"""
        try:
            value = json.loads(value_json)
            
            # 제품 캐시 키 패턴: prod:{id}
            if key.startswith("prod:"):
                product_id = int(key.split(":")[1])
                await self._update_product_in_db(product_id, value)
                logger.info(f"DB write completed for product {product_id}")
            else:
                logger.warning(f"Unknown cache key pattern for DB write: {key}")
                
        except Exception as e:
            logger.error(f"DB write failed for key {key}: {e}")
    
    async def _update_product_in_db(self, product_id: int, product_data: dict):
        """제품 데이터를 MySQL에 업데이트"""
        from ..db.database import get_db_connection
        
        try:
            async with get_db_connection() as conn:
                async with conn.cursor() as cursor:
                    # 현재 DB의 데이터와 캐시 데이터 비교 후 업데이트
                    await cursor.execute(
                        "UPDATE products SET name = %s, price = %s WHERE id = %s",
                        (product_data.get('name'), product_data.get('price'), product_id)
                    )
                    
                    if cursor.rowcount > 0:
                        logger.debug(f"DB updated for product {product_id}")
                    else:
                        logger.warning(f"No rows updated for product {product_id}")
                        
        except Exception as e:
            logger.error(f"Failed to update product {product_id} in DB: {e}")
            raise
    
    async def start_queue_processor(self):
        """백그라운드 큐 처리기 시작"""
        if not self.redis:
            logger.warning("Redis not connected, cannot start queue processor")
            return
        
        logger.info("Starting Write-Behind queue processor...")
        
        async def _process_queue_continuously():
            """큐를 지속적으로 처리하는 백그라운드 태스크"""
            while True:
                try:
                    processed = await self.process_write_behind_queue(batch_size=5)
                    if processed == 0:
                        # 처리할 메시지가 없으면 1초 대기
                        await asyncio.sleep(1)
                    else:
                        # 처리한 메시지가 있으면 즉시 다음 배치 처리
                        await asyncio.sleep(0.1)
                except Exception as e:
                    logger.error(f"Queue processor error: {e}")
                    await asyncio.sleep(5)  # 에러 시 5초 대기
        
        # 백그라운드 태스크로 시작
        asyncio.create_task(_process_queue_continuously())
        logger.info("Queue processor started")


# 전역 캐시 매니저 인스턴스
cache_manager = CacheManager()
