from fastapi import FastAPI

from .db import init_db
from .routers import router as products_router


app = FastAPI(title="Products API", version="1.0.0")


@app.on_event("startup")
def on_startup() -> None:
    init_db()


@app.get("/")
def root() -> dict:
    return {"status": "ok"}


app.include_router(products_router)

# For direct running: uvicorn backend.main:app --reload

