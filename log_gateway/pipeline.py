# -----------------------------------------------------------------------------
# 파일명 : log_gateway/pipeline.py
# 목적   : 서비스별 배치 생성 루프 및 Kafka 퍼블리셔 태스크 구성
# 설명   : generator가 전달한 프로파일/시뮬레이터 기반으로 큐+워커 흐름을 실행
# -----------------------------------------------------------------------------
from __future__ import annotations

import asyncio
import random
from typing import Any, Dict, List, Tuple

from .core.timeband import current_hour_kst, pick_multiplier
from . import producer

# 파이프라인 파라미터
BATCH_MIN = 50
BATCH_MAX = 200
QUEUE_SIZE = 10_000
PUBLISHER_WORKERS = 4


async def _service_loop(
    service: str,
    simulator: Any,
    target_rps: float,
    publish_queue: "asyncio.Queue[Tuple[str, str, bool]]",
    bands: List[Any],
    weight_mode: str,
    batch_range: Tuple[int, int],
) -> None:
    """서비스별로 배치 로그를 생성해 퍼블리시 큐에 쌓는다."""
    batch_min, batch_max = batch_range
    batch_min = max(1, batch_min)
    batch_max = max(batch_min, batch_max)

    while True:
        hour = current_hour_kst()
        multiplier = pick_multiplier(bands, hour_kst=hour, mode=weight_mode) if bands else 1.0
        effective_rps = max(target_rps * multiplier, 0.01)
        batch_size = random.randint(batch_min, batch_max)
        logs = simulator.generate_logs(batch_size)
        for event in logs:
            payload = simulator.render(event)
            is_error = event.get("level") == "ERROR"
            await publish_queue.put((service, payload, is_error))
        sleep_time = batch_size / effective_rps
        if sleep_time > 0:
            await asyncio.sleep(sleep_time)


async def _publisher_worker(
    worker_id: int,
    publish_queue: "asyncio.Queue[Tuple[str, str, bool]]",
    stats_queue: "asyncio.Queue[Tuple[str, int]]",
) -> None:
    """큐에 쌓인 로그를 Kafka에 발행하고 통계를 보고."""
    while True:
        service, payload, is_error = await publish_queue.get()
        try:
            await producer.publish(service, payload, replicate_error=is_error)
            stats_queue.put_nowait((service, 1))
        finally:
            publish_queue.task_done()


def start_pipeline(
    simulators: Dict[str, Any],
    service_rps: Dict[str, float],
    base_rps: float,
    bands: List[Any],
    weight_mode: str,
) -> Tuple[
    "asyncio.Queue[Tuple[str, str, bool]]",
    "asyncio.Queue[Tuple[str, int]]",
    List[asyncio.Task],
    List[asyncio.Task],
]:
    """큐/워커 태스크를 초기화하고 반환."""
    batch_range = (BATCH_MIN, BATCH_MAX)
    publish_queue: "asyncio.Queue[Tuple[str, str, bool]]" = asyncio.Queue(maxsize=QUEUE_SIZE)
    stats_queue: "asyncio.Queue[Tuple[str, int]]" = asyncio.Queue()

    available_services = list(simulators.keys())
    svc_count = max(len(available_services), 1)
    fallback_rps = base_rps / svc_count

    service_tasks = [
        asyncio.create_task(
            _service_loop(
                service=svc,
                simulator=simulators[svc],
                target_rps=service_rps.get(svc, fallback_rps),
                publish_queue=publish_queue,
                bands=bands,
                weight_mode=weight_mode,
                batch_range=batch_range,
            ),
            name=f"service-loop-{svc}",
        )
        for svc in available_services
    ]

    publisher_tasks = [
        asyncio.create_task(
            _publisher_worker(worker_id=i, publish_queue=publish_queue, stats_queue=stats_queue),
            name=f"publisher-{i}",
        )
        for i in range(PUBLISHER_WORKERS)
    ]

    return publish_queue, stats_queue, service_tasks, publisher_tasks
