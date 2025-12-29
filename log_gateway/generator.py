# -----------------------------------------------------------------------------
# 파일명 : log_gateway/generator.py
# 목적   : 서비스별 시뮬레이터/파이프라인을 구성하는 헬퍼 제공
# 사용   : run.py 등에서 build_generation_pipeline() 호출 후 실행
# -----------------------------------------------------------------------------

from __future__ import annotations
import asyncio
import os
from typing import Any, Dict, List, Tuple

from .config.profile_route_settings import load_profile_context
from .simulator.build_simulators import build_simulators
from .simulator_pipeline import create_service_tasks
from .kafka_pipeline import create_publisher_tasks

PROFILE_NAME = "baseline"


def _compute_service_eps(
    total_eps: float,
    mix: Dict[str, Any],
    services: List[str],
    simulator_share: float,
    worker_count: int,
) -> Tuple[float, Dict[str, float]]:
    """전체 EPS를 share/워커로 나눈 뒤 mix 비중으로 서비스별 목표 EPS 계산."""
    if not services:
        return 0.0, {}

    worker_count = max(worker_count, 1)
    simulator_share = max(simulator_share, 0.0)
    base_eps = total_eps * simulator_share / worker_count

    weights = {service: float(mix.get(service, 1.0)) for service in services}
    weight_sum = sum(weights.values())
    if weight_sum <= 0:
        weight_sum = float(len(services))
        weights = {service: 1.0 for service in services}

    service_eps = {service: base_eps * (weights[service] / weight_sum) for service in services}
    return base_eps, service_eps


def build_generation_pipeline(profile_name: str = PROFILE_NAME) -> Tuple[
    List[str],
    asyncio.Queue[Tuple[str, str, bool]],
    asyncio.Queue[Tuple[str, int]],
    List[asyncio.Task],
    List[asyncio.Task],
]:
    """프로파일 기반으로 시뮬레이터/파이프라인을 초기화하고 태스크 목록을 반환."""
    context = load_profile_context(profile_name)
    profile = context.profile
    simulator_share = float(os.getenv("SIMULATOR_SHARE", "1.0"))
    worker_count = int(os.getenv("UVICORN_WORKERS", os.getenv("WEB_CONCURRENCY", "1")))
    mix = context.mix
    weight_mode = context.weight_mode
    bands = context.bands

    # 2) 서비스별 시뮬레이터 인스턴스 생성
    simulators = build_simulators(profile)
    available_services = list(simulators.keys())
    base_eps, service_eps = _compute_service_eps(
        total_eps=context.total_eps,
        mix=mix,
        services=available_services,
        simulator_share=simulator_share,
        worker_count=worker_count,
    )

    publish_queue, service_tasks, _ = create_service_tasks(
        simulators=simulators,
        base_eps=base_eps,
        service_eps=service_eps,
        bands=bands,
        weight_mode=weight_mode,
    )

    stats_queue: "asyncio.Queue[Tuple[str, int]]" = asyncio.Queue()
    publisher_tasks = create_publisher_tasks(
        publish_queue=publish_queue,
        stats_queue=stats_queue,
    )

    return available_services, publish_queue, stats_queue, service_tasks, publisher_tasks
