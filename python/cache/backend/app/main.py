from fastapi import FastAPI

from .core.config import settings
from .db.database import init_database
from .api.v1.api import api_router


def create_app() -> FastAPI:
    """FastAPI 애플리케이션 팩토리"""
    app = FastAPI(
        title=settings.APP_NAME,
        version=settings.APP_VERSION,
        debug=settings.DEBUG,
    )

    @app.on_event("startup")
    def on_startup() -> None:
        """애플리케이션 시작 시 실행"""
        init_database()

    @app.get("/")
    def root() -> dict:
        """헬스 체크 엔드포인트"""
        return {"status": "ok", "version": settings.APP_VERSION}

    # API v1 라우터 포함
    app.include_router(api_router, prefix=settings.API_V1_STR)

    return app


# 애플리케이션 인스턴스 생성
app = create_app()
