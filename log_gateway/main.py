# -----------------------------------------------------------------------------
# 파일명 : log_gateway/main.py
# 목적   : FastAPI 엔트리포인트로 /logs 및 /simulate API와 런타임 로그 제너레이터를 구동
# 설명   : Uvicorn으로 기동 시 startup 이벤트에서 run_simulator() 백그라운드 태스크를 시작함
# -----------------------------------------------------------------------------

from __future__ import annotations

import asyncio
from fastapi import FastAPI

from .run import run_simulator

app = FastAPI()


@app.get("/ping")
async def ping():
    return {"status": "ok"}

@app.on_event("startup")
async def start_generator() -> None:
    # API 서버 기동과 함께 시뮬레이터 루프를 백그라운드로 시작한다.
    asyncio.create_task(run_simulator())
