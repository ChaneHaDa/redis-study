from __future__ import annotations

from typing import Generator

from sqlite3 import Connection

from .config import settings
from ..db.database import get_db_connection


def get_database() -> Generator[Connection, None, None]:
    """데이터베이스 연결 의존성"""
    with get_db_connection() as conn:
        yield conn
