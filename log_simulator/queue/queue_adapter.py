# -----------------------------------------------------------------------------
# file: log_simulator/queue/queue_adapter.py
# purpose: queue helpers with bounded overflow policy support
# -----------------------------------------------------------------------------

from __future__ import annotations

import asyncio
import queue as std_queue
from typing import Any, Tuple


def queue_maxsize(publish_queue: Any, fallback: int = 0) -> int:
    """큐 maxsize를 조회하고 없으면 fallback을 사용한다."""
    maxsize = getattr(publish_queue, "maxsize", 0)
    if maxsize:
        return int(maxsize)
    return int(fallback) if fallback else 0


def queue_qsize(publish_queue: Any) -> int:
    """큐의 현재 크기를 안전하게 조회한다."""
    try:
        return int(publish_queue.qsize())
    except (NotImplementedError, AttributeError):
        return 0


def queue_put_nowait(publish_queue: Any, batch_items: list[Any]) -> None:
    """큐에 배치를 논블로킹으로 넣는다."""
    if hasattr(publish_queue, "put_nowait"):
        publish_queue.put_nowait(batch_items)
        return
    publish_queue.put(batch_items, block=False)


def queue_get_nowait(publish_queue: Any) -> Any:
    """큐에서 항목을 논블로킹으로 꺼낸다."""
    if hasattr(publish_queue, "get_nowait"):
        return publish_queue.get_nowait()
    return publish_queue.get(block=False)


def enqueue_batch(
    publish_queue: Any,
    batch_items: list[Any],
    overflow_policy: str,
    *,
    fallback_maxsize: int = 0,
) -> Tuple[int, int]:
    """배치를 큐에 적재하고 overflow 정책을 적용한다."""
    if not batch_items:
        return 0, 0

    maxsize = queue_maxsize(publish_queue, fallback=fallback_maxsize)
    if maxsize <= 0:
        queue_put_nowait(publish_queue, batch_items)
        return len(batch_items), 0

    try:
        queue_put_nowait(publish_queue, batch_items)
        return len(batch_items), 0
    except (asyncio.QueueFull, std_queue.Full):
        if overflow_policy == "drop_newest":
            return 0, len(batch_items)

    dropped = 0
    for _ in range(5):
        try:
            old = queue_get_nowait(publish_queue)
            if isinstance(old, list):
                dropped += len(old)
            else:
                dropped += 1
        except (asyncio.QueueEmpty, std_queue.Empty):
            break

        try:
            queue_put_nowait(publish_queue, batch_items)
            return len(batch_items), dropped
        except (asyncio.QueueFull, std_queue.Full):
            continue

    return 0, dropped + len(batch_items)
