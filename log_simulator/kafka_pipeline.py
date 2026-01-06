# -----------------------------------------------------------------------------
# 파일명 : log_simulator/kafka_pipeline.py
# 목적   : 시뮬레이터 큐에서 로그를 읽어 Kafka에 전송하는 워커를 구성
# -----------------------------------------------------------------------------

from __future__ import annotations

import asyncio
from collections import Counter
import logging
import time
from typing import List, Tuple

from .producer import publish_batch_direct
from .models.messages import BatchMessage
from .config.settings import PUBLISHER_SETTINGS

# 퍼블리셔 기본 설정
PUBLISHER_WORKERS = PUBLISHER_SETTINGS.workers
WORKER_BATCH_SIZE = PUBLISHER_SETTINGS.worker_batch_size
QUEUE_WARN_RATIO = PUBLISHER_SETTINGS.queue_warn_ratio
IDLE_WARN_SEC = PUBLISHER_SETTINGS.idle_warn_sec
SEND_WARN_SEC = PUBLISHER_SETTINGS.send_warn_sec
RETRY_BACKOFF_SEC = PUBLISHER_SETTINGS.retry_backoff_sec


_logger = logging.getLogger("log_simulator.kafka_pipeline")
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
        batch: list[BatchMessage] = []
        consumed_batches = 0

        wait_start = time.perf_counter()
        first = await publish_queue.get()
        wait_duration = time.perf_counter() - wait_start
        consumed_batches += 1
        batch.extend(first)

        # 여러 배치를 합쳐 전송 오버헤드를 줄인다.
        while len(batch) < WORKER_BATCH_SIZE:
            try:
                # 여러 배치를 합쳐 publish overhead를 줄인다.
                nxt = publish_queue.get_nowait()
                consumed_batches += 1
                batch.extend(nxt)
            except asyncio.QueueEmpty:
                break

        send_ok = True
        send_start = time.perf_counter()
        try:
            await publish_batch_direct(batch)
        except Exception:
            send_ok = False
            _logger.exception(
                "[publisher] send failed worker=%d batch=%d",
                worker_id,
                len(batch),
            )
            if RETRY_BACKOFF_SEC > 0:
                await asyncio.sleep(RETRY_BACKOFF_SEC)
        send_duration = time.perf_counter() - send_start

        queue_depth = publish_queue.qsize()
        queue_capacity = publish_queue.maxsize

        if wait_duration > IDLE_WARN_SEC:
            _logger.info(
                "[publisher] idle worker=%d wait=%.3fs queue=%d",
                worker_id,
                wait_duration,
                queue_depth,
            )

        if send_duration > SEND_WARN_SEC:
            _logger.info(
                "[publisher] slow send worker=%d batch=%d duration=%.3fs",
                worker_id,
                len(batch),
                send_duration,
            )

        if queue_capacity and queue_capacity > 0:
            fill_ratio = queue_depth / queue_capacity
            if fill_ratio >= QUEUE_WARN_RATIO:
                _logger.info(
                    "[publisher] queue backlog worker=%d queue=%d/%d (%.0f%%)",
                    worker_id,
                    queue_depth,
                    queue_capacity,
                    fill_ratio * 100,
                )

        # 성공한 배치만 stats에 반영해 오탐을 줄인다.
        if send_ok:
            svc_counter = Counter(message.service for message in batch)
            for svc, cnt in svc_counter.items():
                stats_queue.put_nowait((svc, cnt))

        for _ in range(consumed_batches):
            publish_queue.task_done()


def create_publisher_tasks(
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
