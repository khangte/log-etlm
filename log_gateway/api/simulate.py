# -----------------------------------------------------------------------------
# 파일명 : log_gateway/api/simulate.py
# 목적   : 시뮬레이터 클래스를 직접 호출해 샘플 로그를 생성하고 Kafka로 보낼 수 있는 관리용 API
# 설명   : /simulate/{service}?count=N 호출 시 제너레이터와 동일한 프로파일을 공유하며 이벤트를 생성
# -----------------------------------------------------------------------------

from __future__ import annotations

from typing import Any, Dict
from fastapi import APIRouter, HTTPException, Query

from ..core.config import PROFILES_DIR, load_profile
from ..generator import _build_simulators, PROFILE_NAME
from .. import producer
from ..simulator.base import BaseServiceSimulator


router = APIRouter(prefix="/simulate", tags=["simulate"])

_profile: Dict[str, Any] = load_profile(PROFILES_DIR / f"{PROFILE_NAME}.yaml")
_simulators: Dict[str, Any] = _build_simulators(_profile)


@router.get("/{service}")
async def simulate_once(
    service: str,
    count: int = Query(
        1,
        ge=1,
        le=100,
        description="생성해서 Kafka로 보낼 로그 개수",
    ),
) -> Dict[str, Any]:
    """
    /simulate/{service}?count=N
    → 해당 서비스 시뮬레이터로 로그 N개 생성 후 Kafka로 전송.
    """
    simulator = _simulators.get(service)
    if simulator is None:
        raise HTTPException(status_code=404, detail=f"unknown service: {service}")

    generated = 0
    for _ in range(count):
        event: Dict[str, Any] = simulator.generate_log_one()
        payload = simulator.render(event)
        await producer.publish(service, payload)
        generated += 1

    return {
        "service": service,
        "generated": generated,
        "ts": BaseServiceSimulator.now_kst_iso(),
    }
