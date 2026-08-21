# -----------------------------------------------------------------------------
# 파일명 : log_simulator/simulator/task_builder.py
# 목적   : 시뮬레이터 태스크 생성
# -----------------------------------------------------------------------------

from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Tuple

from .settings import get_queue_config, get_simulator_settings
from .stream_pipeline import run_simulator_loop
from ..models.messages import BatchMessage


def create_simulator_tasks(
    simulators: Dict[str, Any],
    base_eps: float,
    service_eps: Dict[str, float],
) -> Tuple["asyncio.Queue[list[BatchMessage]]", List[asyncio.Task]]:
    settings = get_simulator_settings()
    queue_config = get_queue_config()

    publish_queue: "asyncio.Queue[list[BatchMessage]]" = asyncio.Queue(
        maxsize=settings.queue_size
    )

    available_services = list(simulators.keys())
    service_count = max(len(available_services), 1)
    fallback_eps = base_eps / service_count

    loops = max(int(settings.loops_per_service), 1)
    service_tasks: List[asyncio.Task] = []
    for service in available_services:
        target = service_eps.get(service, fallback_eps)
        per_loop_eps = target / loops
        for idx in range(loops):
            task = asyncio.create_task(
                run_simulator_loop(
                    service=service,
                    simulator=simulators[service],
                    target_eps=per_loop_eps,
                    publish_queue=publish_queue,
                    log_batch_size=settings.log_batch_size,
                    settings=settings,
                    queue_config=queue_config,
                ),
                name=f"service-loop-{service}-{idx}",
            )
            service_tasks.append(task)

    return publish_queue, service_tasks


def create_service_tasks_only(
    simulators: Dict[str, Any],
    base_eps: float,
    service_eps: Dict[str, float],
    publish_queue: "asyncio.Queue[list[BatchMessage]]",
) -> List[asyncio.Task]:
    """기존 publish_queue를 재사용해 service_tasks만 새로 생성한다. EPS 실시간 변경용."""
    settings = get_simulator_settings()
    queue_config = get_queue_config()

    available_services = list(simulators.keys())
    service_count = max(len(available_services), 1)
    fallback_eps = base_eps / service_count
    loops = max(int(settings.loops_per_service), 1)

    service_tasks: List[asyncio.Task] = []
    for service in available_services:
        target = service_eps.get(service, fallback_eps)
        per_loop_eps = target / loops
        for idx in range(loops):
            task = asyncio.create_task(
                run_simulator_loop(
                    service=service,
                    simulator=simulators[service],
                    target_eps=per_loop_eps,
                    publish_queue=publish_queue,
                    log_batch_size=settings.log_batch_size,
                    settings=settings,
                    queue_config=queue_config,
                ),
                name=f"service-loop-{service}-{idx}",
            )
            service_tasks.append(task)

    return service_tasks
