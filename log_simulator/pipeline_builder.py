# -----------------------------------------------------------------------------
# 파일명 : log_simulator/pipeline_builder.py
# 목적   : 시뮬레이터/퍼블리셔 파이프라인 조립만 담당
# 사용   : engine.py 등에서 assemble_pipeline() 호출
# -----------------------------------------------------------------------------

from __future__ import annotations
import asyncio
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

from .simulator_pipeline import create_service_tasks
from .kafka_pipeline import create_publisher_tasks
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
    weight_mode: str,
) -> Pipeline:
    """큐/태스크를 조립해 반환한다."""
    publish_queue, service_tasks = create_service_tasks(
        simulators=simulators,
        base_eps=base_eps,
        service_eps=service_eps,
        bands=bands,
        weight_mode=weight_mode,
    )

    # 퍼블리셔가 stats를 넣고 리포터가 소비하는 큐.
    stats_queue: "asyncio.Queue[Tuple[str, int]]" = asyncio.Queue()
    publisher_tasks = create_publisher_tasks(
        publish_queue=publish_queue,
        stats_queue=stats_queue,
    )

    return Pipeline(
        publish_queue=publish_queue,
        stats_queue=stats_queue,
        service_tasks=service_tasks,
        publisher_tasks=publisher_tasks,
    )
