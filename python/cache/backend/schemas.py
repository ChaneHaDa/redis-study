from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, validator


class ProductBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    price: float = Field(..., ge=0)

    @validator("name")
    def strip_name(cls, v: str) -> str:  # noqa: N805
        s = v.strip()
        if not s:
            raise ValueError("name must not be empty")
        return s


class ProductCreate(ProductBase):
    pass


class ProductUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    price: Optional[float] = Field(None, ge=0)

    @validator("name")
    def strip_optional_name(cls, v: Optional[str]) -> Optional[str]:  # noqa: N805
        if v is None:
            return v
        s = v.strip()
        if not s:
            raise ValueError("name must not be empty")
        return s


class Product(ProductBase):
    id: int
    updated_at: datetime


