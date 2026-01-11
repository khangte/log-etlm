# -----------------------------------------------------------------------------
# 파일명 : log_simulator/simulator/task_builder.py
# 목적   : 시뮬레이터 태스크 생성
# -----------------------------------------------------------------------------

from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Tuple

from .settings import SIMULATOR_SETTINGS
from .stream_pipeline import run_simulator_loop
from ..models.messages import BatchMessage


LOG_BATCH_SIZE = SIMULATOR_SETTINGS.log_batch_size
QUEUE_SIZE = SIMULATOR_SETTINGS.queue_size
LOOPS_PER_SERVICE = SIMULATOR_SETTINGS.loops_per_service


def create_simulator_tasks(
    simulators: Dict[str, Any],
    base_eps: float,
    service_eps: Dict[str, float],
    bands: List[Any],
    log_batch_size: int = LOG_BATCH_SIZE,
    queue_size: int = QUEUE_SIZE,
    loops_per_service: int = LOOPS_PER_SERVICE,
) -> Tuple["asyncio.Queue[list[BatchMessage]]", List[asyncio.Task]]:
    """시뮬레이터 태스크들을 생성하고 publish 큐와 함께 반환."""
    publish_queue: "asyncio.Queue[list[BatchMessage]]" = asyncio.Queue(maxsize=queue_size)

    available_services = list(simulators.keys())
    service_count = max(len(available_services), 1)
    fallback_eps = base_eps / service_count

    loops = max(loops_per_service, 1)
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
                    bands=bands,
                    log_batch_size=log_batch_size,
                ),
                name=f"service-loop-{service}-{idx}",
            )
            service_tasks.append(task)

    return publish_queue, service_tasks
