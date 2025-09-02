from __future__ import annotations

from typing import List

from fastapi import APIRouter, HTTPException, status

from . import repository
from .schemas import Product, ProductCreate, ProductUpdate


router = APIRouter(prefix="/products", tags=["products"])


@router.post("", response_model=Product, status_code=status.HTTP_201_CREATED)
def create_product(payload: ProductCreate) -> Product:
    created = repository.create_product(name=payload.name, price=payload.price)
    return Product(**created)


@router.get("", response_model=List[Product])
def list_products() -> List[Product]:
    items = repository.list_products()
    return [Product(**i) for i in items]


@router.get("/{product_id}", response_model=Product)
def get_product(product_id: int) -> Product:
    item = repository.get_product(product_id)
    if item is None:
        raise HTTPException(status_code=404, detail="Product not found")
    return Product(**item)


@router.put("/{product_id}", response_model=Product)
def update_product(product_id: int, payload: ProductUpdate) -> Product:
    if payload.name is None and payload.price is None:
        raise HTTPException(status_code=400, detail="No fields to update")
    updated = repository.update_product(product_id, payload.name, payload.price)
    if updated is None:
        raise HTTPException(status_code=404, detail="Product not found")
    return Product(**updated)


@router.delete("/{product_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_product(product_id: int) -> None:
    ok = repository.delete_product(product_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Product not found")
    return None


