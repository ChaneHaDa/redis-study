from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from typing import Generator

from ..core.config import settings


@contextmanager
def get_db_connection() -> Generator[sqlite3.Connection, None, None]:
    """데이터베이스 연결 컨텍스트 매니저"""
    conn = sqlite3.connect(settings.DATABASE_PATH, check_same_thread=False)  # 스레드 안전성 해제 (캐시 테스트용)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_database() -> None:
    """데이터베이스 초기화"""
    settings.DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with get_db_connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                price REAL NOT NULL,
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
            )
            """
        )