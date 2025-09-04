from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict

from pydantic import validator
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """애플리케이션 설정"""
    
    # 애플리케이션 기본 설정
    APP_NAME: str = "Products API"
    APP_VERSION: str = "1.0.0"
    DEBUG: bool = False
    
    # API 설정
    API_V1_STR: str = "/api/v1"
    
    # 데이터베이스 설정
    DATABASE_URL: str = "sqlite:///./products.db"
    DATABASE_PATH: Path = Path(__file__).resolve().parents[3] / "products.db"
    
    # 서버 설정
    HOST: str = "127.0.0.1"
    PORT: int = 8000
    
    # Redis 설정
    REDIS_URL: str = "redis://localhost:6379/0"
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    REDIS_PASSWORD: str = ""
    
    # 캐시 설정
    CACHE_TTL: int = 300  # 5분
    CACHE_JITTER_PERCENT: int = 10  # TTL ±10% 지터
    
    @validator("DATABASE_PATH", pre=True)
    def resolve_database_path(cls, v: Path) -> Path:
        """데이터베이스 경로를 절대 경로로 변환"""
        return v.resolve()
    
    class Config:
        env_file = ".env"
        case_sensitive = True


# 전역 설정 인스턴스
settings = Settings()
