from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import List, Optional

from ..models.schemas import Product, ProductCreate, ProductUpdate


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
    
    def get_product(self, product_id: int) -> Optional[Product]:
        """제품 상세 조회"""
        row = self.db.execute(
            "SELECT id, name, price, updated_at FROM products WHERE id = ?",
            (product_id,),
        ).fetchone()
        return None if row is None else Product(**_row_to_product(row))
    
    def update_product(self, product_id: int, product_data: ProductUpdate) -> Optional[Product]:
        """제품 업데이트"""
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

        cur = self.db.execute(
            f"UPDATE products SET {', '.join(fields)} WHERE id = ?", 
            tuple(params)
        )
        if cur.rowcount == 0:
            return None
            
        row = self.db.execute(
            "SELECT id, name, price, updated_at FROM products WHERE id = ?",
            (product_id,),
        ).fetchone()
        assert row is not None
        return Product(**_row_to_product(row))
    
    def delete_product(self, product_id: int) -> bool:
        """제품 삭제"""
        cur = self.db.execute("DELETE FROM products WHERE id = ?", (product_id,))
        return cur.rowcount > 0
