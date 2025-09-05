from __future__ import annotations

import asyncio
import logging
import sqlite3
from datetime import datetime
from typing import List, Optional

from ..core.cache import cache_manager
from ..models.schemas import Product, ProductCreate, ProductUpdate

logger = logging.getLogger(__name__)


def _dict_from_row(row: sqlite3.Row) -> dict:
    """SQLite Row를 딕셔너리로 변환"""
    return {key: row[key] for key in row.keys()}


def _row_to_product(row: sqlite3.Row) -> dict:
    """SQLite Row를 제품 딕셔너리로 변환"""
    data = _dict_from_row(row)
    data["updated_at"] = datetime.fromisoformat(
        data["updated_at"].replace("Z", "+00:00").replace(" ", "T")
    )
    return data


class ProductService:
    """제품 관련 비즈니스 로직 서비스"""
    
    def __init__(self, db_connection: sqlite3.Connection):
        self.db = db_connection
    
    def _get_cache_key(self, product_id: int) -> str:
        """캐시 키 생성: prod:{id}"""
        return f"prod:{product_id}"
    
    def create_product(self, product_data: ProductCreate) -> Product:
        """제품 생성"""
        cur = self.db.execute(
            "INSERT INTO products (name, price) VALUES (?, ?)", 
            (product_data.name, product_data.price)
        )
        product_id = cur.lastrowid
        row = self.db.execute(
            "SELECT id, name, price, updated_at FROM products WHERE id = ?",
            (product_id,),
        ).fetchone()
        assert row is not None
        return Product(**_row_to_product(row))
    
    def list_products(self) -> List[Product]:
        """제품 목록 조회"""
        rows = self.db.execute(
            "SELECT id, name, price, updated_at FROM products ORDER BY id DESC"
        ).fetchall()
        return [Product(**_row_to_product(r)) for r in rows]
    
    async def get_product(self, product_id: int) -> Optional[Product]:
        """제품 상세 조회 - Singleflight 패턴으로 캐시 스탬피드 방지"""
        cache_key = self._get_cache_key(product_id)
        
        async def _fetch_from_db() -> Optional[dict]:
            """DB에서 제품 데이터 조회하는 fallback 함수"""
            logger.debug(f"Fetching product {product_id} from DB")
            
            def _query_db():
                """DB 조회 동기 함수 - 새 연결 사용"""
                from ..db.database import get_db_connection
                with get_db_connection() as conn:
                    row = conn.execute(
                        "SELECT id, name, price, updated_at FROM products WHERE id = ?",
                        (product_id,),
                    ).fetchone()
                    return row
            
            # 동기 DB 작업을 thread executor에서 비동기로 실행
            try:
                loop = asyncio.get_event_loop()
                row = await loop.run_in_executor(None, _query_db)
                
                if row is None:
                    return None
                
                # DB 데이터를 딕셔너리로 변환
                product_data = _row_to_product(row)
                return product_data
                
            except Exception as e:
                logger.error(f"DB query error for product {product_id}: {e}")
                # DB 에러 시 예외를 다시 발생시켜 500 에러로 처리
                raise
        
        # Singleflight 패턴으로 캐시 스탬피드 방지
        product_data = await cache_manager.get_with_singleflight(
            key=cache_key,
            fallback=_fetch_from_db
        )
        
        if product_data is None:
            return None
            
        return Product(**product_data)
    
    async def update_product(self, product_id: int, product_data: ProductUpdate) -> Optional[Product]:
        """제품 업데이트 - 캐시 무효화 포함"""
        fields: list[str] = []
        params: list[object] = []
        
        if product_data.name is not None:
            fields.append("name = ?")
            params.append(product_data.name)
        if product_data.price is not None:
            fields.append("price = ?")
            params.append(product_data.price)
        
        if not fields:
            return None
            
        fields.append("updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')")
        params.append(product_id)

        def _update_db():
            """DB 업데이트 동기 함수 - 새 연결 사용"""
            from ..db.database import get_db_connection
            with get_db_connection() as conn:
                cur = conn.execute(
                    f"UPDATE products SET {', '.join(fields)} WHERE id = ?", 
                    tuple(params)
                )
                if cur.rowcount == 0:
                    return None
                
                row = conn.execute(
                    "SELECT id, name, price, updated_at FROM products WHERE id = ?",
                    (product_id,),
                ).fetchone()
                return row
        
        # 동기 DB 작업을 thread executor에서 비동기로 실행
        loop = asyncio.get_event_loop()
        row = await loop.run_in_executor(None, _update_db)
        
        if row is None:
            return None
        
        # 캐시 무효화
        cache_key = self._get_cache_key(product_id)
        await cache_manager.delete(cache_key)
        logger.debug(f"Cache invalidated for product {product_id}")
        
        return Product(**_row_to_product(row))
    
    async def delete_product(self, product_id: int) -> bool:
        """제품 삭제 - 캐시 무효화 포함"""
        def _delete_db():
            """DB 삭제 동기 함수 - 새 연결 사용"""
            from ..db.database import get_db_connection
            with get_db_connection() as conn:
                cur = conn.execute("DELETE FROM products WHERE id = ?", (product_id,))
                return cur.rowcount > 0
        
        # 동기 DB 작업을 thread executor에서 비동기로 실행
        loop = asyncio.get_event_loop()
        deleted = await loop.run_in_executor(None, _delete_db)
        
        if deleted:
            # 캐시 무효화
            cache_key = self._get_cache_key(product_id)
            await cache_manager.delete(cache_key)
            logger.debug(f"Cache invalidated for deleted product {product_id}")
            return True
        
        return False
