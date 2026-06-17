# -----------------------------------------------------------------------------
# 파일명 : log_simulator/api/simulator.py
# 목적   : 시뮬레이터 제어 API 라우터
# -----------------------------------------------------------------------------

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from ..engine import engine

router = APIRouter(prefix="/simulator", tags=["simulator"])


@router.get("/ping")
async def ping():
    """헬스체크."""
    return {"status": "ok"}


@router.patch("/eps")
async def update_eps(value: float):
    """실행 중 EPS를 실시간으로 변경한다."""
    if value <= 0:
        raise HTTPException(status_code=422, detail="EPS는 0보다 커야 합니다.")
    try:
        await engine.set_eps(value)
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
    return {"status": "ok", "eps": value}
