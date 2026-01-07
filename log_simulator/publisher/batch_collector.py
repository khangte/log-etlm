# -----------------------------------------------------------------------------
# file: log_simulator/publisher/batch_collector.py
# purpose: collect a batch from publish queue using size or wait constraints
# -----------------------------------------------------------------------------

from __future__ import annotations

import queue as std_queue
import time
from typing import Any


def collect_batch(
    publish_queue: Any,
    batch_size: int,
    batch_wait_sec: float,
    stop_event: Any | None,
) -> list[Any] | None:
    try:
        first = publish_queue.get(timeout=0.5)
    except std_queue.Empty:
        if stop_event is not None and stop_event.is_set():
            return None
        return []

    if first is None:
        return None

    batch: list[Any] = []
    batch.extend(first)
    if not batch:
        return []

    deadline = time.perf_counter() + batch_wait_sec
    while len(batch) < batch_size:
        remaining = deadline - time.perf_counter()
        if remaining <= 0:
            break
        try:
            nxt = publish_queue.get(timeout=remaining)
        except std_queue.Empty:
            break
        if nxt is None:
            break
        batch.extend(nxt)
    return batch
