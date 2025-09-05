from __future__ import annotations

from typing import AsyncGenerator

import aiomysql

from .config import settings
from ..db.database import get_db_connection


async def get_database() -> AsyncGenerator[aiomysql.Connection, None]:
    """데이터베이스 연결 의존성"""
    async with get_db_connection() as conn:
        yield conn
