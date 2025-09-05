from fastapi import FastAPI

from .core.config import settings
from .core.cache import cache_manager
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
    async def on_startup() -> None:
        """애플리케이션 시작 시 실행"""
        await init_database()
        await cache_manager.connect()
        await cache_manager.start_queue_processor()  # Write-Behind 큐 처리기 시작
    
    @app.on_event("shutdown")
    async def on_shutdown() -> None:
        """애플리케이션 종료 시 실행"""
        await cache_manager.disconnect()

    @app.get("/")
    def root() -> dict:
        """헬스 체크 엔드포인트"""
        return {"status": "ok", "version": settings.APP_VERSION}
    
    @app.get("/metrics")
    def get_cache_metrics() -> dict:
        """캐시 메트릭 엔드포인트"""
        metrics = cache_manager.get_metrics()
        # Write-Behind 큐 정보 추가
        return {
            **metrics,
            "write_patterns": {
                "current": cache_manager.get_write_pattern().value,
                "available": ["invalidation", "write_through", "write_behind"]
            }
        }

    # API v1 라우터 포함
    app.include_router(api_router, prefix=settings.API_V1_STR)

    return app


# 애플리케이션 인스턴스 생성
app = create_app()
