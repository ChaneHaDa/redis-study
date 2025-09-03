from fastapi import APIRouter

from .products import router as products_router

api_router = APIRouter()

# API v1 라우터들을 포함
api_router.include_router(products_router)
