# -----------------------------------------------------------------------------
# 파일명 : log_simulator/simulator/task_builder.py
# 목적   : 시뮬레이터 태스크 생성
# -----------------------------------------------------------------------------

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

from .settings import (
    QueueThrottleConfig,
    SimulatorSettings,
    get_queue_config,
    get_simulator_settings,
)
from .stream_pipeline import run_simulator_loop
from ..models.messages import BatchMessage


@dataclass(frozen=True)
class SimulatorTaskBuilder:
    """시뮬레이터 태스크 빌더를 담는다."""
    settings: SimulatorSettings
    queue_config: QueueThrottleConfig

    def create_tasks(
        self,
        simulators: Dict[str, Any],
        base_eps: float,
        service_eps: Dict[str, float],
        bands: List[Any],
        *,
        log_batch_size: int | None = None,
        queue_size: int | None = None,
        loops_per_service: int | None = None,
    ) -> Tuple["asyncio.Queue[list[BatchMessage]]", List[asyncio.Task]]:
        """시뮬레이터 태스크들을 생성하고 publish 큐와 함께 반환."""
        resolved_log_batch_size = (
            self.settings.log_batch_size
            if log_batch_size is None
            else log_batch_size
        )
        resolved_queue_size = (
            self.settings.queue_size if queue_size is None else queue_size
        )
        resolved_loops = (
            self.settings.loops_per_service
            if loops_per_service is None
            else loops_per_service
        )

        publish_queue: "asyncio.Queue[list[BatchMessage]]" = asyncio.Queue(
            maxsize=resolved_queue_size
        )

        available_services = list(simulators.keys())
        service_count = max(len(available_services), 1)
        fallback_eps = base_eps / service_count

        loops = max(int(resolved_loops), 1)
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
                        log_batch_size=resolved_log_batch_size,
                        settings=self.settings,
                        queue_config=self.queue_config,
                    ),
                    name=f"service-loop-{service}-{idx}",
                )
                service_tasks.append(task)

        return publish_queue, service_tasks


def create_simulator_tasks(
    simulators: Dict[str, Any],
    base_eps: float,
    service_eps: Dict[str, float],
    bands: List[Any],
    log_batch_size: int | None = None,
    queue_size: int | None = None,
    loops_per_service: int | None = None,
    *,
    settings: SimulatorSettings | None = None,
    queue_config: QueueThrottleConfig | None = None,
) -> Tuple["asyncio.Queue[list[BatchMessage]]", List[asyncio.Task]]:
    resolved_settings = settings or get_simulator_settings()
    resolved_queue_config = queue_config or get_queue_config(resolved_settings)
    builder = SimulatorTaskBuilder(
        settings=resolved_settings,
        queue_config=resolved_queue_config,
    )
    return builder.create_tasks(
        simulators,
        base_eps,
        service_eps,
        bands,
        log_batch_size=log_batch_size,
        queue_size=queue_size,
        loops_per_service=loops_per_service,
    )
