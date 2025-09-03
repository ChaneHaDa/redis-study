from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, validator


class ProductBase(BaseModel):
    """제품 기본 스키마"""
    name: str = Field(..., min_length=1, max_length=255, description="제품명")
    price: float = Field(..., ge=0, description="가격")

    @validator("name")
    def strip_name(cls, v: str) -> str:  # noqa: N805
        s = v.strip()
        if not s:
            raise ValueError("name must not be empty")
        return s


class ProductCreate(ProductBase):
    """제품 생성 스키마"""
    pass


class ProductUpdate(BaseModel):
    """제품 업데이트 스키마"""
    name: Optional[str] = Field(None, min_length=1, max_length=255, description="제품명")
    price: Optional[float] = Field(None, ge=0, description="가격")

    @validator("name")
    def strip_optional_name(cls, v: Optional[str]) -> Optional[str]:  # noqa: N805
        if v is None:
            return v
        s = v.strip()
        if not s:
            raise ValueError("name must not be empty")
        return s


class Product(ProductBase):
    """제품 응답 스키마"""
    id: int = Field(..., description="제품 ID")
    updated_at: datetime = Field(..., description="업데이트 시간")
