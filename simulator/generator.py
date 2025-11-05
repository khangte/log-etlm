# -----------------------------------------------------------------------------
# 파일명 : simulator/generator.py
# 목적   : 서비스별 시뮬레이터 클래스를 찾아 배치(log 이벤트 리스트)를 생성
# 사용   : runner에서 generate_batch(service, count, routes, profile) 호출
# -----------------------------------------------------------------------------

from __future__ import annotations
from typing import Any, Dict, List
from simulator.services import REGISTRY


def generate_batch(
    service: str,
    count: int,
    routes: List[Dict[str, Any]],
    profile: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    서비스명으로 등록된 시뮬레이터 클래스를 찾아 인스턴스를 만들고,
    지정된 개수만큼 이벤트를 생성한다.

    Args:
        service: "auth" | "order" | "payment" | "notify"
        count: 생성할 이벤트 개수
        routes: 해당 서비스의 라우트 리스트 (templates/routes.yml)
        profile: 실행 프로파일(dict). error_rate 등 포함.

    Returns:
        List[Dict[str, Any]]: 최소 9-키 스키마의 이벤트 리스트

    Raises:
        ValueError: 알 수 없는 서비스명일 때
    """
    cls = REGISTRY.get(service)
    if cls is None:
        raise ValueError(f"unknown service: {service}")
    sim = cls(routes=routes, profile=profile)
    return sim.generate_batch(count)
