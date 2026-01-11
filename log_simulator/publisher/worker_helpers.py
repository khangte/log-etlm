# -----------------------------------------------------------------------------
# 파일명 : log_simulator/publisher/worker_helpers.py
# 목적   : 퍼블리셔 워커 공통 헬퍼
# -----------------------------------------------------------------------------

from __future__ import annotations

import asyncio
import time
from collections import Counter
from dataclasses import dataclass
from typing import Tuple

from ..models.messages import BatchMessage


@dataclass(frozen=True)
class WorkerMetricsConfig:
    idle_warn_sec: float
    send_warn_sec: float
    queue_warn_ratio: float


async def collect_batch(
    publish_queue: "asyncio.Queue[list[BatchMessage]]",
    max_batch_size: int,
) -> tuple[list[BatchMessage], int, float]:
    """Collect batches from the queue until max size or queue is empty."""
    batch: list[BatchMessage] = []
    consumed_batches = 0
    wait_start = time.perf_counter()
    first = await publish_queue.get()
    wait_duration = time.perf_counter() - wait_start
    consumed_batches += 1
    batch.extend(first)

    while len(batch) < max_batch_size:
        try:
            nxt = publish_queue.get_nowait()
            consumed_batches += 1
            batch.extend(nxt)
        except asyncio.QueueEmpty:
            break
    return batch, consumed_batches, wait_duration


def log_worker_metrics(
    logger,
    *,
    config: WorkerMetricsConfig,
    worker_id: int,
    wait_duration: float,
    send_duration: float,
    batch_size: int,
    queue_depth: int,
    queue_capacity: int,
) -> None:
    """Log idle/slow/backlog metrics for the worker."""
    if wait_duration > config.idle_warn_sec:
        logger.info(
            "[publisher] idle worker=%d wait=%.3fs queue=%d",
            worker_id,
            wait_duration,
            queue_depth,
        )

    if send_duration > config.send_warn_sec:
        logger.info(
            "[publisher] slow send worker=%d batch=%d duration=%.3fs",
            worker_id,
            batch_size,
            send_duration,
        )

    if queue_capacity and queue_capacity > 0:
        fill_ratio = queue_depth / queue_capacity
        if fill_ratio >= config.queue_warn_ratio:
            logger.info(
                "[publisher] queue backlog worker=%d queue=%d/%d (%.0f%%)",
                worker_id,
                queue_depth,
                queue_capacity,
                fill_ratio * 100,
            )


def push_stats(
    stats_queue: "asyncio.Queue[Tuple[str, int]]",
    batch: list[BatchMessage],
) -> None:
    """Push per-service counts into stats queue."""
    svc_counter = Counter(message.service for message in batch)
    for svc, cnt in svc_counter.items():
        stats_queue.put_nowait((svc, cnt))
