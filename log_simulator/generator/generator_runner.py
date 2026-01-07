# -----------------------------------------------------------------------------
# file: log_simulator/generator/generator_runner.py
# purpose: generator runner (async) for split generator entrypoint
# -----------------------------------------------------------------------------

from __future__ import annotations

import asyncio
from typing import Any

from ..config.profile_route_settings import load_profile_context
from ..config.settings import SIMULATOR_SETTINGS
from .eps_allocation import compute_service_eps
from ..simulator.build_simulators import build_simulators
from .generator_pipeline import create_service_tasks


async def run_generator_async(
    publish_queue: Any,
    metrics_queue: Any | None,
    stop_event: Any | None,
) -> None:
    """시뮬레이터 컨텍스트를 로드해 서비스 루프를 실행한다."""
    context = load_profile_context()
    context.profile["event_mode"] = SIMULATOR_SETTINGS.event_mode
    simulators = build_simulators(context.profile)
    base_eps, service_eps = compute_service_eps(
        total_eps=context.total_eps,
        mix=context.mix,
        services=list(simulators.keys()),
        simulator_share=SIMULATOR_SETTINGS.simulator_share,
    )

    _, service_tasks = create_service_tasks(
        simulators=simulators,
        base_eps=base_eps,
        service_eps=service_eps,
        bands=context.bands,
        weight_mode=context.weight_mode,
        publish_queue=publish_queue,
        metrics_queue=metrics_queue,
        overflow_policy=SIMULATOR_SETTINGS.overflow_policy,
        stop_event=stop_event,
    )

    await asyncio.gather(*service_tasks)
