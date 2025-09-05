from __future__ import annotations

from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
import aiomysql

from ...core.cache import cache_manager, WritePattern
from ...core.dependencies import get_database
from ...models.schemas import Product, ProductCreate, ProductUpdate
from ...services.product_service import ProductService

router = APIRouter(prefix="/products", tags=["products"])


@router.post("", response_model=Product, status_code=status.HTTP_201_CREATED)
async def create_product(
    payload: ProductCreate,
    db: aiomysql.Connection = Depends(get_database)
) -> Product:
    """제품 생성"""
    service = ProductService(db)
    return await service.create_product(payload)


@router.get("", response_model=List[Product])
async def list_products(
    db: aiomysql.Connection = Depends(get_database)
) -> List[Product]:
    """제품 목록 조회"""
    service = ProductService(db)
    return await service.list_products()


@router.get("/{product_id}", response_model=Product)
async def get_product(
    product_id: int,
    db: aiomysql.Connection = Depends(get_database)
) -> Product:
    """제품 상세 조회 - 캐시 적용"""
    service = ProductService(db)
    product = await service.get_product(product_id)
    if product is None:
        raise HTTPException(status_code=404, detail="Product not found")
    return product


@router.put("/{product_id}", response_model=Product)
async def update_product(
    product_id: int,
    payload: ProductUpdate,
    write_pattern: Optional[str] = Query(None, description="Write pattern: invalidation, write_through, write_behind"),
    db: aiomysql.Connection = Depends(get_database)
) -> Product:
    """제품 업데이트 - 3가지 쓰기 패턴 지원
    
    Query Parameters:
    - write_pattern: invalidation (기본값) | write_through | write_behind
    """
    if payload.name is None and payload.price is None:
        raise HTTPException(status_code=400, detail="No fields to update")
    
    # 쓰기 패턴 파싱
    pattern = None
    if write_pattern:
        try:
            pattern = WritePattern(write_pattern.lower())
        except ValueError:
            raise HTTPException(
                status_code=400, 
                detail=f"Invalid write pattern: {write_pattern}. Use: invalidation, write_through, write_behind"
            )
    
    service = ProductService(db)
    updated = await service.update_product(product_id, payload, write_pattern=pattern)
    if updated is None:
        raise HTTPException(status_code=404, detail="Product not found")
    return updated


@router.delete("/{product_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_product(
    product_id: int,
    db: aiomysql.Connection = Depends(get_database)
):
    """제품 삭제"""
    service = ProductService(db)
    ok = await service.delete_product(product_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Product not found")


@router.post("/config/write-pattern")
async def set_write_pattern(
    pattern: str = Query(..., description="Write pattern: invalidation, write_through, write_behind")
) -> dict:
    """전역 쓰기 패턴 설정"""
    try:
        write_pattern = WritePattern(pattern.lower())
        cache_manager.set_write_pattern(write_pattern)
        return {
            "message": f"Write pattern set to: {write_pattern.value}",
            "pattern": write_pattern.value
        }
    except ValueError:
        raise HTTPException(
            status_code=400, 
            detail=f"Invalid write pattern: {pattern}. Use: invalidation, write_through, write_behind"
        )


@router.get("/config/write-pattern")
async def get_write_pattern() -> dict:
    """현재 쓰기 패턴 조회"""
    current_pattern = cache_manager.get_write_pattern()
    return {
        "pattern": current_pattern.value,
        "description": {
            "invalidation": "DB 커밋 후 캐시 삭제",
            "write_through": "DB 쓰기 성공 후 바로 캐시 업데이트",
            "write_behind": "캐시 먼저 업데이트, DB는 큐를 통해 비동기"
        }[current_pattern.value]
    }
