# -----------------------------------------------------------------------------
# 파일명 : log_gateway/kafka_pipeline.py
# 목적   : 시뮬레이터 큐에서 로그를 읽어 Kafka에 전송하는 워커를 구성
# -----------------------------------------------------------------------------

from __future__ import annotations

import asyncio
from collections import Counter
import logging
import os
import time
from typing import List, Tuple

from .producer import BatchMessage, get_producer, publish_batch_direct

# 퍼블리셔 기본 설정
# 10k RPS = (워커 12개 × 워커당 833 RPS × 배치 100건)
PUBLISHER_WORKERS = int(os.getenv("LG_PUBLISHER_WORKERS", "8"))
WORKER_BATCH_SIZE = int(os.getenv("LG_WORKER_BATCH_SIZE", "80"))
QUEUE_WARN_RATIO = float(os.getenv("LG_PUBLISH_QUEUE_WARN_RATIO", os.getenv("LG_QUEUE_WARN_RATIO", "0.7")))
IDLE_WARN_SEC = float(os.getenv("LG_IDLE_WARN_SEC", "0.2"))
SEND_WARN_SEC = float(os.getenv("LG_SEND_WARN_SEC", "0.3"))


_logger = logging.getLogger("log_gateway.kafka_pipeline")
_logger.setLevel(logging.INFO)
if not _logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    )
    _logger.addHandler(handler)


async def _publisher_worker(
    worker_id: int,
    publish_queue: "asyncio.Queue[list[Tuple[str, str, bool]]]",
    stats_queue: "asyncio.Queue[Tuple[str, int]]",
) -> None:
    """큐에 쌓인 로그를 Kafka에 발행."""
    producer = get_producer()

    while True:
        batch: list[Tuple[str, str, bool]] = []
        consumed_batches = 0

        wait_start = time.perf_counter()
        first = await publish_queue.get()
        wait_duration = time.perf_counter() - wait_start
        consumed_batches += 1
        batch.extend(first)

        while len(batch) < WORKER_BATCH_SIZE:
            try:
                nxt = publish_queue.get_nowait()
                consumed_batches += 1
                batch.extend(nxt)
            except asyncio.QueueEmpty:
                break

        messages = [
            BatchMessage(service, payload, None, err) for (service, payload, err) in batch
        ]
        send_start = time.perf_counter()
        await publish_batch_direct(messages)
        send_duration = time.perf_counter() - send_start

        producer.poll(0)

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

        svc_counter = Counter(svc for svc, _, _ in batch)
        for svc, cnt in svc_counter.items():
            stats_queue.put_nowait((svc, cnt))

        for _ in range(consumed_batches):
            publish_queue.task_done()


def create_publisher_tasks(
    publish_queue: "asyncio.Queue[list[Tuple[str, str, bool]]]",
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
