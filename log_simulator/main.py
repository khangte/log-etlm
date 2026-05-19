# -----------------------------------------------------------------------------
# 파일명 : log_simulator/main.py
# 목적   : FastAPI 엔트리포인트로 /logs 및 /simulate API와 런타임 로그 제너레이터를 구동
# 설명   : Uvicorn으로 기동 시 lifespan에서 엔진을 시작/중지함
# -----------------------------------------------------------------------------

from __future__ import annotations

import logging
import logging.config
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from fastapi import FastAPI

logging.config.dictConfig({
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {
            "format": "%(asctime)s %(levelname)-8s %(name)s %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "default",
            "stream": "ext://sys.stdout",
        },
    },
    "loggers": {
        "log_simulator": {
            "handlers": ["console"],
            "level": "INFO",
            "propagate": False,
        },
    },
})

from .engine import engine


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
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
