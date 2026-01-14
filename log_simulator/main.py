# -----------------------------------------------------------------------------
# 파일명 : log_simulator/main.py
# 목적   : FastAPI 엔트리포인트로 /logs 및 /simulate API와 런타임 로그 제너레이터를 구동
# 설명   : Uvicorn으로 기동 시 lifespan에서 엔진을 시작/중지함
# -----------------------------------------------------------------------------

from __future__ import annotations

from contextlib import asynccontextmanager
from fastapi import FastAPI

from .engine import engine


@asynccontextmanager
async def lifespan(app: FastAPI):
    """lifespan 처리를 수행한다."""
    await engine.start()
    try:
        yield
    finally:
        await engine.stop()


app = FastAPI(lifespan=lifespan)


@app.get("/ping")
async def ping():
    """ping 처리를 수행한다."""
    return {"status": "ok"}
