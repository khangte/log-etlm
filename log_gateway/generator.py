# -----------------------------------------------------------------------------
# 파일명 : log_gateway/generator.py
# 목적   : 서비스별 시뮬레이터를 인스턴스화하고 FastAPI 앱 생명주기 동안 지속적으로 로그를 생성/Kafka로 전송
# 사용   : main.py startup 이벤트에서 run_generator()를 asyncio task로 띄워 백그라운드 발행
# -----------------------------------------------------------------------------

from __future__ import annotations
from typing import Any, Dict, List
import asyncio

from .config.profile_route_settings import load_profile_context
from .config.stats import stats_reporter
from .simulator.build_simulators import build_simulators
from .pipeline import start_pipeline

PROFILE_NAME = "baseline"


def _compute_service_rps(base_rps: float, mix: Dict[str, Any], services: List[str]) -> Dict[str, float]:
    """mix 비중을 기반으로 서비스별 목표 RPS 계산."""
    if not services:
        return {}

    weights = {svc: float(mix.get(svc, 1.0)) for svc in services}
    weight_sum = sum(weights.values())
    if weight_sum <= 0:
        weight_sum = float(len(services))
        weights = {svc: 1.0 for svc in services}

    return {svc: base_rps * (weights[svc] / weight_sum) for svc in services}


# -----------------------------------------------------------------------------
# 비동기 제너레이터: 앱 살아있는 동안 계속 로그 생성
# -----------------------------------------------------------------------------

async def run_generator() -> None:
    """
    앱이 떠 있는 동안 계속 로그를 생성해서 Kafka로 발행하는 비동기 제너레이터.

    FastAPI main.py 예시:

        @app.on_event("startup")
        async def start_generator():
            asyncio.create_task(run_generator())
    """
    # logger.info("[generator] starting run_generator()")

    # 1) 프로파일 컨텍스트 로딩
    context = load_profile_context(PROFILE_NAME)
    profile = context.profile
    base_rps = context.base_rps
    mix = context.mix
    weight_mode = context.weight_mode
    bands = context.bands

    # 2) 서비스별 시뮬레이터 인스턴스 생성
    simulators = build_simulators(profile)
    available_services = list(simulators.keys())
    service_rps = _compute_service_rps(base_rps, mix, available_services)

    (
        publish_queue, stats_queue, service_tasks, publisher_tasks,
    ) = start_pipeline(
        simulators=simulators,
        base_rps=base_rps,
        bands=bands,
        service_rps=service_rps,
        weight_mode=weight_mode,
    )

    stats_task = asyncio.create_task(
        stats_reporter(stats_queue=stats_queue, services=available_services),
        name="stats-reporter",
    )

    tasks = service_tasks + publisher_tasks + [stats_task]
    try:
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            task.cancel()
