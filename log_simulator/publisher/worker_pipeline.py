# -----------------------------------------------------------------------------
# 파일명 : log_simulator/publisher/worker_pipeline.py
# 목적   : 시뮬레이터 큐에서 로그를 읽어 Kafka에 전송하는 워커를 구성
# -----------------------------------------------------------------------------

from __future__ import annotations

import asyncio
import logging
import time
from typing import List, Tuple

from .dlq import publish_dlq_batch
from ..producer.client import publish_batch_direct
from ..models.messages import BatchMessage
from .settings import PUBLISHER_SETTINGS
from .worker_helpers import (
    WorkerMetricsConfig,
    collect_batch,
    log_worker_metrics,
    push_stats,
)

# 퍼블리셔 기본 설정
PUBLISHER_WORKERS = PUBLISHER_SETTINGS.workers
WORKER_BATCH_SIZE = PUBLISHER_SETTINGS.worker_batch_size
QUEUE_WARN_RATIO = PUBLISHER_SETTINGS.queue_warn_ratio
IDLE_WARN_SEC = PUBLISHER_SETTINGS.idle_warn_sec
SEND_WARN_SEC = PUBLISHER_SETTINGS.send_warn_sec
RETRY_BACKOFF_SEC = PUBLISHER_SETTINGS.retry_backoff_sec
METRICS_CONFIG = WorkerMetricsConfig(
    idle_warn_sec=IDLE_WARN_SEC,
    send_warn_sec=SEND_WARN_SEC,
    queue_warn_ratio=QUEUE_WARN_RATIO,
)


_logger = logging.getLogger("log_simulator.publisher.worker_pipeline")
_logger.setLevel(logging.INFO)
if not _logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    )
    _logger.addHandler(handler)


async def _publisher_worker(
    worker_id: int,
    publish_queue: "asyncio.Queue[list[BatchMessage]]",
    stats_queue: "asyncio.Queue[Tuple[str, int]]",
) -> None:
    """큐에 쌓인 로그를 Kafka에 발행."""
    while True:
        batch, consumed_batches, wait_duration = await collect_batch(
            publish_queue,
            WORKER_BATCH_SIZE,
        )

        send_ok = True
        send_start = time.perf_counter()
        try:
            await publish_batch_direct(batch)
        except Exception as exc:
            send_ok = False
            _logger.exception(
                "[publisher] send failed worker=%d batch=%d",
                worker_id,
                len(batch),
            )
            await publish_dlq_batch(batch, exc)
            if RETRY_BACKOFF_SEC > 0:
                await asyncio.sleep(RETRY_BACKOFF_SEC)
        send_duration = time.perf_counter() - send_start

        queue_depth = publish_queue.qsize()
        queue_capacity = publish_queue.maxsize

        log_worker_metrics(
            _logger,
            config=METRICS_CONFIG,
            worker_id=worker_id,
            wait_duration=wait_duration,
            send_duration=send_duration,
            batch_size=len(batch),
            queue_depth=queue_depth,
            queue_capacity=queue_capacity,
        )

        # 성공한 배치만 stats에 반영해 오탐을 줄인다.
        if send_ok:
            push_stats(stats_queue, batch)

        for _ in range(consumed_batches):
            publish_queue.task_done()


def create_publisher_workers(
    publish_queue: "asyncio.Queue[list[BatchMessage]]",
    stats_queue: "asyncio.Queue[Tuple[str, int]]",
    worker_count: int = PUBLISHER_WORKERS,
) -> List[asyncio.Task]:
    """Kafka 퍼블리셔 워커 태스크 생성."""
    return [
        asyncio.create_task(
            _publisher_worker(worker_id=i, publish_queue=publish_queue, stats_queue=stats_queue),
            name=f"publisher-{i}",
        )
        for i in range(worker_count)
    ]
