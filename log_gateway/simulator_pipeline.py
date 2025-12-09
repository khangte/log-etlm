# -----------------------------------------------------------------------------
# 파일명 : log_gateway/simulator_pipeline.py
# 목적   : 서비스별 시뮬레이터 배치 생성 루프를 구성하고 큐에 로그를 적재
# -----------------------------------------------------------------------------

from __future__ import annotations

import asyncio
import time
from typing import Any, Dict, List, Tuple

from .config.timeband import current_hour_kst, pick_multiplier

# 서비스 루프 기본 설정
LOG_BATCH_SIZE: int = 200
QUEUE_SIZE: int = 10000


async def _service_stream_loop(
    service: str,
    simulator: Any,
    target_rps: float,
    publish_queue: "asyncio.Queue[Tuple[str, str, bool]]",
    bands: List[Any],
    weight_mode: str,
    log_batch_size: int,
) -> None:
    """서비스별로 배치 로그를 생성해 퍼블리시 큐에 쌓는다."""
    while True:
        loop_start = time.perf_counter()
        hour = current_hour_kst()
        multiplier = pick_multiplier(bands, hour_kst=hour, mode=weight_mode) if bands else 1.0
        effective_rps = max(target_rps * multiplier, 0.01)
        batch_size = log_batch_size

        logs = simulator.generate_logs(batch_size)
        for log in logs:
            payload = simulator.render(log)
            await publish_queue.put((service, payload, log.get("level") == "ERROR"))

        desired_period = batch_size / effective_rps
        elapsed = time.perf_counter() - loop_start
        sleep_time = max(0.0, desired_period - elapsed)

        if sleep_time > 0:
            await asyncio.sleep(sleep_time)


def create_service_tasks(
    simulators: Dict[str, Any],
    base_rps: float,
    service_rps: Dict[str, float],
    bands: List[Any],
    weight_mode: str,
    log_batch_size: int = LOG_BATCH_SIZE,
    queue_size: int = QUEUE_SIZE,
) -> Tuple["asyncio.Queue[Tuple[str, str, bool]]", List[asyncio.Task], List[str]]:
    """시뮬레이터 태스크들을 생성하고 publish 큐와 함께 반환."""
    publish_queue: "asyncio.Queue[Tuple[str, str, bool]]" = asyncio.Queue(maxsize=queue_size)

    available_services = list(simulators.keys())
    service_count = max(len(available_services), 1)
    fallback_rps = base_rps / service_count

    service_tasks = [
        asyncio.create_task(
            _service_stream_loop(
                service=service,
                simulator=simulators[service],
                target_rps=service_rps.get(service, fallback_rps),
                publish_queue=publish_queue,
                bands=bands,
                weight_mode=weight_mode,
                log_batch_size=log_batch_size,
            ),
            name=f"service-loop-{service}",
        )
        for service in available_services
    ]

    return publish_queue, service_tasks, available_services
