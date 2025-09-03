from __future__ import annotations

from typing import List

from fastapi import APIRouter, Depends, HTTPException, status
from sqlite3 import Connection

from ...core.dependencies import get_database
from ...models.schemas import Product, ProductCreate, ProductUpdate
from ...services.product_service import ProductService

router = APIRouter(prefix="/products", tags=["products"])


@router.post("", response_model=Product, status_code=status.HTTP_201_CREATED)
def create_product(
    payload: ProductCreate,
    db: Connection = Depends(get_database)
) -> Product:
    """제품 생성"""
    service = ProductService(db)
    return service.create_product(payload)


@router.get("", response_model=List[Product])
def list_products(
    db: Connection = Depends(get_database)
) -> List[Product]:
    """제품 목록 조회"""
    service = ProductService(db)
    return service.list_products()


@router.get("/{product_id}", response_model=Product)
def get_product(
    product_id: int,
    db: Connection = Depends(get_database)
) -> Product:
    """제품 상세 조회"""
    service = ProductService(db)
    product = service.get_product(product_id)
    if product is None:
        raise HTTPException(status_code=404, detail="Product not found")
    return product


@router.put("/{product_id}", response_model=Product)
def update_product(
    product_id: int,
    payload: ProductUpdate,
    db: Connection = Depends(get_database)
) -> Product:
    """제품 업데이트"""
    if payload.name is None and payload.price is None:
        raise HTTPException(status_code=400, detail="No fields to update")
    
    service = ProductService(db)
    updated = service.update_product(product_id, payload)
    if updated is None:
        raise HTTPException(status_code=404, detail="Product not found")
    return updated


@router.delete("/{product_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_product(
    product_id: int,
    db: Connection = Depends(get_database)
):
    """제품 삭제"""
    service = ProductService(db)
    ok = service.delete_product(product_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Product not found")
