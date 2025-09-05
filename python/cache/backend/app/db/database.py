from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncGenerator

import aiomysql
import asyncio
from ..core.config import settings


@asynccontextmanager
async def get_db_connection() -> AsyncGenerator[aiomysql.Connection, None]:
    """데이터베이스 연결 컨텍스트 매니저"""
    conn = None
    try:
        conn = await aiomysql.connect(
            host=settings.MYSQL_HOST,
            port=settings.MYSQL_PORT,
            user=settings.MYSQL_USER,
            password=settings.MYSQL_PASSWORD,
            db=settings.MYSQL_DATABASE,
            charset='utf8mb4',
            autocommit=False
        )
        yield conn
        if conn:
            await conn.commit()
    except Exception as e:
        if conn:
            await conn.rollback()
        raise e
    finally:
        if conn:
            conn.close()  # aiomysql의 close()는 동기 메서드입니다


async def init_database() -> None:
    """데이터베이스 초기화"""
    async with get_db_connection() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS products (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    price DECIMAL(10, 2) NOT NULL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                )
                """
            )