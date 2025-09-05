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
    
    # 데이터베이스 설정 (MySQL)
    DATABASE_URL: str = "mysql+aiomysql://cache_user:cache_password@localhost:3306/cache_study"
    MYSQL_HOST: str = "localhost"
    MYSQL_PORT: int = 3306
    MYSQL_USER: str = "cache_user"
    MYSQL_PASSWORD: str = "cache_password"
    MYSQL_DATABASE: str = "cache_study"
    
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
    CACHE_TTL: int = 3  # 3초 (캐시 스탬피드 테스트용)
    CACHE_JITTER_PERCENT: int = 10  # TTL ±10% 지터
    
    
    class Config:
        env_file = ".env"
        case_sensitive = True


# 전역 설정 인스턴스
settings = Settings()
