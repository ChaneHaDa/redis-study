from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import List, Optional

import aiomysql

from ..core.cache import cache_manager, WritePattern
from ..models.schemas import Product, ProductCreate, ProductUpdate

logger = logging.getLogger(__name__)


async def _fetch_one_to_dict(cursor: aiomysql.Cursor) -> Optional[dict]:
    """MySQL cursor result를 딕셔너리로 변환"""
    row = await cursor.fetchone()
    if row is None:
        return None
    
    columns = [desc[0] for desc in cursor.description]
    return dict(zip(columns, row))


async def _fetch_all_to_dicts(cursor: aiomysql.Cursor) -> List[dict]:
    """MySQL cursor results를 딕셔너리 리스트로 변환"""
    rows = await cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row)) for row in rows]


def _row_to_product_data(row_dict: dict) -> dict:
    """MySQL row 딕셔너리를 제품 데이터로 변환"""
    data = row_dict.copy()
    # MySQL TIMESTAMP를 datetime 객체로 유지 (이미 datetime 객체임)
    if isinstance(data.get("updated_at"), datetime):
        data["updated_at"] = data["updated_at"]
    return data


class ProductService:
    """제품 관련 비즈니스 로직 서비스"""
    
    def __init__(self, db_connection: aiomysql.Connection):
        self.db = db_connection
    
    def _get_cache_key(self, product_id: int) -> str:
        """캐시 키 생성: prod:{id}"""
        return f"prod:{product_id}"
    
    async def create_product(self, product_data: ProductCreate) -> Product:
        """제품 생성"""
        async with self.db.cursor() as cursor:
            await cursor.execute(
                "INSERT INTO products (name, price) VALUES (%s, %s)", 
                (product_data.name, product_data.price)
            )
            product_id = cursor.lastrowid
            
            await cursor.execute(
                "SELECT id, name, price, updated_at FROM products WHERE id = %s",
                (product_id,),
            )
            row_dict = await _fetch_one_to_dict(cursor)
            
        if row_dict is None:
            raise ValueError(f"Failed to retrieve created product with id {product_id}")
            
        return Product(**_row_to_product_data(row_dict))
    
    async def list_products(self) -> List[Product]:
        """제품 목록 조회"""
        async with self.db.cursor() as cursor:
            await cursor.execute(
                "SELECT id, name, price, updated_at FROM products ORDER BY id DESC"
            )
            rows = await _fetch_all_to_dicts(cursor)
            
        return [Product(**_row_to_product_data(r)) for r in rows]
    
    async def get_product(self, product_id: int) -> Optional[Product]:
        """제품 상세 조회 - 캐시 적용"""
        cache_key = self._get_cache_key(product_id)
        
        async def _fetch_from_db() -> Optional[dict]:
            """DB에서 제품 데이터 조회하는 fallback 함수"""
            logger.debug(f"Fetching product {product_id} from DB")
            
            try:
                async with self.db.cursor() as cursor:
                    await cursor.execute(
                        "SELECT id, name, price, updated_at FROM products WHERE id = %s",
                        (product_id,),
                    )
                    row_dict = await _fetch_one_to_dict(cursor)
                    
                if row_dict is None:
                    return None
                
                # DB 데이터를 딕셔너리로 변환
                product_data = _row_to_product_data(row_dict)
                return product_data
                
            except Exception as e:
                logger.error(f"DB query error for product {product_id}: {e}")
                raise
        
        # 1. 캐시에서 조회 시도
        cached_data = await cache_manager.get(cache_key)
        logger.info(f"READ: Cache get result for key={cache_key}: {cached_data}")
        if cached_data is not None:
            logger.info(f"Cache hit for product {product_id}, returning cached data")
            return Product(**cached_data)
        
        # 2. 캐시 미스 - DB에서 조회
        logger.debug(f"Cache miss for product {product_id}, querying DB")
        product_data = await _fetch_from_db()
        
        if product_data is None:
            return None
        
        # 3. DB 데이터를 캐시에 저장 (정상 조회된 경우에만)
        await cache_manager.set(cache_key, product_data)
        
        return Product(**product_data)
    
    async def update_product(self, product_id: int, product_data: ProductUpdate, write_pattern: WritePattern = None) -> Optional[Product]:
        """제품 업데이트 - 3가지 쓰기 패턴 지원"""
        fields: list[str] = []
        params: list[object] = []
        
        if product_data.name is not None:
            fields.append("name = %s")
            params.append(product_data.name)
        if product_data.price is not None:
            fields.append("price = %s")
            params.append(product_data.price)
        
        if not fields:
            return None
            
        params.append(product_id)
        
        cache_key = self._get_cache_key(product_id)
        
        # 쓰기 패턴 결정 (파라미터로 받거나 전역 설정 사용)
        current_pattern = write_pattern or cache_manager.get_write_pattern()
        logger.info(f"Using write pattern: {current_pattern.value} for product {product_id}")

        if current_pattern == WritePattern.WRITE_BEHIND:
            # Write-Behind: 캐시 먼저 업데이트
            return await self._handle_write_behind_update(product_id, product_data, fields, params, cache_key)
        else:
            # Invalidation, Write-Through: DB 먼저 업데이트
            return await self._handle_db_first_update(product_id, fields, params, cache_key, current_pattern)
    
    async def _handle_db_first_update(self, product_id: int, fields: list[str], params: list[object], 
                                     cache_key: str, pattern: WritePattern) -> Optional[Product]:
        """DB 먼저 업데이트하는 패턴 (Invalidation, Write-Through)"""
        
        async with self.db.cursor() as cursor:
            await cursor.execute(
                f"UPDATE products SET {', '.join(fields)} WHERE id = %s", 
                tuple(params)
            )
            
            if cursor.rowcount == 0:
                return None
            
            await cursor.execute(
                "SELECT id, name, price, updated_at FROM products WHERE id = %s",
                (product_id,),
            )
            row_dict = await _fetch_one_to_dict(cursor)
        
        if row_dict is None:
            return None
        
        product_data_dict = _row_to_product_data(row_dict)
        
        # 쓰기 패턴에 따른 캐시 처리
        if pattern == WritePattern.INVALIDATION:
            # Invalidation: 캐시 삭제
            await cache_manager.handle_write_invalidation(cache_key)
            logger.debug(f"INVALIDATION: Cache deleted for product {product_id}")
            
        elif pattern == WritePattern.WRITE_THROUGH:
            # Write-Through: 캐시 업데이트
            await cache_manager.set(cache_key, product_data_dict)
            cache_manager.metrics["write_through"] += 1
            logger.debug(f"WRITE-THROUGH: Cache updated for product {product_id}")
        
        return Product(**product_data_dict)
    
    async def _handle_write_behind_update(self, product_id: int, product_data: ProductUpdate, 
                                         fields: list[str], params: list[object], cache_key: str) -> Optional[Product]:
        """Write-Behind: 캐시 먼저 업데이트, DB는 비동기"""
        
        # 1. 기존 데이터 조회 (캐시 우선)
        cached_data = await cache_manager.get(cache_key)
        
        if cached_data is None:
            # 캐시에 없으면 DB에서 조회
            async with self.db.cursor() as cursor:
                await cursor.execute(
                    "SELECT id, name, price, updated_at FROM products WHERE id = %s",
                    (product_id,),
                )
                row_dict = await _fetch_one_to_dict(cursor)
            
            if row_dict is None:
                return None
            
            cached_data = _row_to_product_data(row_dict)
        
        # 2. 캐시 데이터 업데이트
        updated_data = cached_data.copy()
        if product_data.name is not None:
            updated_data["name"] = product_data.name
        if product_data.price is not None:
            updated_data["price"] = product_data.price
        updated_data["updated_at"] = datetime.now()
        
        # 3. Write-Behind 패턴: 캐시 먼저 업데이트, DB는 큐를 통해 처리
        cache_result = await cache_manager.handle_write_behind(cache_key, updated_data)
        logger.info(f"WRITE-BEHIND: Cache updated and DB write queued for key={cache_key}")
        
        # 즉시 캐시에서 읽어서 확인
        verify_cache = await cache_manager.get(cache_key)
        logger.info(f"WRITE-BEHIND: Cache verification read result={verify_cache}")
        
        return Product(**updated_data)
    
    
    async def delete_product(self, product_id: int) -> bool:
        """제품 삭제 - 캐시 무효화 포함"""
        
        async with self.db.cursor() as cursor:
            await cursor.execute("DELETE FROM products WHERE id = %s", (product_id,))
            deleted = cursor.rowcount > 0
        
        if deleted:
            # 캐시 무효화
            cache_key = self._get_cache_key(product_id)
            await cache_manager.delete(cache_key)
            logger.debug(f"Cache invalidated for deleted product {product_id}")
            return True
        
        return False