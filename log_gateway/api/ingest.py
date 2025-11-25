# -----------------------------------------------------------------------------
# 파일명 : log_gateway/api/ingest.py
# 목적   : 외부에서 POST된 JSON 로그를 Kafka logs.{service} 토픽으로 프록시하는 REST 엔드포인트
# 설명   : /logs/{service} 경로만 제공하며 바디를 그대로 직렬화해 producer.publish 로 전송
# -----------------------------------------------------------------------------

from __future__ import annotations

import json
from typing import Any, Dict

from fastapi import APIRouter, Body

from .. import producer
from ..simulator.base import BaseServiceSimulator


router = APIRouter(prefix="/logs", tags=["logs"])


@router.post("/{service}")
async def ingest_log(
    service: str,
    body: Dict[str, Any] = Body(
        ...,
        description="수집할 로그 JSON(원본 그대로 Kafka로 전달)",
    ),
) -> Dict[str, Any]:
    """
    외부 서비스 → /logs/{service} 로 JSON POST
    → 그대로 Kafka logs.{service} 토픽으로 전달.
    """
    payload = json.dumps(body, ensure_ascii=False)
    await producer.publish(service, payload)

    return {
        "service": service,
        "status": "queued",
        "timestamp": BaseServiceSimulator.now_kst_iso(),
    }
