from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import List, Optional

from .db import db_connection


def _dict_from_row(row: sqlite3.Row) -> dict:
    return {key: row[key] for key in row.keys()}


def _row_to_product(row: sqlite3.Row) -> dict:
    data = _dict_from_row(row)
    data["updated_at"] = datetime.fromisoformat(
        data["updated_at"].replace("Z", "+00:00").replace(" ", "T")
    )
    return data


def create_product(name: str, price: float) -> dict:
    with db_connection() as conn:
        cur = conn.execute(
            "INSERT INTO products (name, price) VALUES (?, ?)", (name, price)
        )
        product_id = cur.lastrowid
        row = conn.execute(
            "SELECT id, name, price, updated_at FROM products WHERE id = ?",
            (product_id,),
        ).fetchone()
        assert row is not None
        return _row_to_product(row)


def list_products() -> List[dict]:
    with db_connection() as conn:
        rows = conn.execute(
            "SELECT id, name, price, updated_at FROM products ORDER BY id DESC"
        ).fetchall()
        return [_row_to_product(r) for r in rows]


def get_product(product_id: int) -> Optional[dict]:
    with db_connection() as conn:
        row = conn.execute(
            "SELECT id, name, price, updated_at FROM products WHERE id = ?",
            (product_id,),
        ).fetchone()
        return None if row is None else _row_to_product(row)


def update_product(product_id: int, name: Optional[str], price: Optional[float]) -> Optional[dict]:
    fields: list[str] = []
    params: list[object] = []
    if name is not None:
        fields.append("name = ?")
        params.append(name)
    if price is not None:
        fields.append("price = ?")
        params.append(price)
    fields.append("updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')")
    params.append(product_id)

    with db_connection() as conn:
        cur = conn.execute(
            f"UPDATE products SET {', '.join(fields)} WHERE id = ?", tuple(params)
        )
        if cur.rowcount == 0:
            return None
        row = conn.execute(
            "SELECT id, name, price, updated_at FROM products WHERE id = ?",
            (product_id,),
        ).fetchone()
        assert row is not None
        return _row_to_product(row)


def delete_product(product_id: int) -> bool:
    with db_connection() as conn:
        cur = conn.execute("DELETE FROM products WHERE id = ?", (product_id,))
        return cur.rowcount > 0


