# -----------------------------------------------------------------------------
# 파일명 : log_simulator/pipeline_builder.py
# 목적   : 시뮬레이터/퍼블리셔 파이프라인 조립만 담당
# 사용   : engine.py 등에서 assemble_pipeline() 호출
# -----------------------------------------------------------------------------

from __future__ import annotations
import asyncio
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

from .simulator.task_builder import create_simulator_tasks
from .simulator.settings import QueueThrottleConfig, SimulatorSettings
from .publisher.settings import PublisherSettings
from .publisher.dlq import DlqPublisher
from .producer.kafka_client import KafkaProducerClient
from .publisher.worker_pipeline import create_publisher_workers
from .models.messages import BatchMessage


@dataclass(frozen=True)
class Pipeline:
    # 파이프라인 구성요소를 명확히 묶어 호출부 실수를 줄인다.
    publish_queue: asyncio.Queue[list[BatchMessage]]
    stats_queue: asyncio.Queue[Tuple[str, int]]
    service_tasks: list[asyncio.Task]
    publisher_tasks: list[asyncio.Task]


def assemble_pipeline(
    simulators: Dict[str, Any],
    base_eps: float,
    service_eps: Dict[str, float],
    bands: List[Any],
    *,
    simulator_settings: SimulatorSettings | None = None,
    queue_config: QueueThrottleConfig | None = None,
    publisher_settings: PublisherSettings | None = None,
    producer: KafkaProducerClient | None = None,
    dlq_publisher: DlqPublisher | None = None,
    worker_count: int | None = None,
    log_batch_size: int | None = None,
    queue_size: int | None = None,
    loops_per_service: int | None = None,
) -> Pipeline:
    """큐/태스크를 조립해 반환한다."""
    publish_queue, service_tasks = create_simulator_tasks(
        simulators=simulators,
        base_eps=base_eps,
        service_eps=service_eps,
        bands=bands,
        log_batch_size=log_batch_size,
        queue_size=queue_size,
        loops_per_service=loops_per_service,
        settings=simulator_settings,
        queue_config=queue_config,
    )

    # 퍼블리셔가 stats를 넣고 리포터가 소비하는 큐.
    stats_queue: "asyncio.Queue[Tuple[str, int]]" = asyncio.Queue()
    publisher_tasks = create_publisher_workers(
        publish_queue=publish_queue,
        stats_queue=stats_queue,
        worker_count=worker_count,
        settings=publisher_settings,
        producer=producer,
        dlq_publisher=dlq_publisher,
    )

    return Pipeline(
        publish_queue=publish_queue,
        stats_queue=stats_queue,
        service_tasks=service_tasks,
        publisher_tasks=publisher_tasks,
    )
