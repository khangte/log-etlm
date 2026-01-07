# -----------------------------------------------------------------------------
# file: log_simulator/publisher/publisher_runner.py
# purpose: publisher worker runner for split publisher entrypoint
# -----------------------------------------------------------------------------

from __future__ import annotations

from collections import Counter, defaultdict
import logging
import threading
import time
from typing import Any, Dict, Tuple

from ..config.settings import PUBLISHER_SETTINGS, SIMULATOR_SETTINGS
from .batch_collector import collect_batch
from .dlq import publish_dlq_batch_sync
from ..queue.metrics_keys import (
    PUBLISHED_TOTAL,
    PUBLISH_FAIL_DROP_TOTAL,
    PUBLISH_FAIL_TOTAL,
    PUBLISH_LATENCY_COUNT,
    PUBLISH_LATENCY_SUM_MS,
    QUEUE_WAIT_COUNT,
    QUEUE_WAIT_SUM_MS,
)
from ..queue.metrics_sink import emit_metric
from .producer import close_producer, publish_batch_direct_sync
from ..queue.queue_adapter import queue_maxsize, queue_qsize


_logger = logging.getLogger("log_simulator.publisher_runner")
_logger.setLevel(logging.INFO)
if not _logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    )
    _logger.addHandler(handler)


def publisher_worker(
    worker_id: int,
    publish_queue: Any,
    metrics_queue: Any | None,
    stop_event: Any | None,
) -> None:
    batch_size = max(PUBLISHER_SETTINGS.batch_size, 1)
    batch_wait_sec = max(PUBLISHER_SETTINGS.batch_wait_sec, 0.0)
    retry_max = max(PUBLISHER_SETTINGS.retry_max, 0)
    retry_backoff_sec = max(PUBLISHER_SETTINGS.retry_backoff_sec, 0.0)

    last_queue_warn_ts = 0.0
    while True:
        batch = collect_batch(publish_queue, batch_size, batch_wait_sec, stop_event)
        if batch is None:
            break
        if not batch:
            continue

        svc_counter = Counter(message.service for message in batch)
        publish_start_ms = int(time.time() * 1000)
        send_duration_ms, send_ok, last_exc = _send_batch(
            batch=batch,
            svc_counter=svc_counter,
            metrics_queue=metrics_queue,
            retry_max=retry_max,
            retry_backoff_sec=retry_backoff_sec,
        )

        if send_ok:
            _emit_publish_metrics(
                metrics_queue=metrics_queue,
                svc_counter=svc_counter,
                send_duration_ms=send_duration_ms,
            )
        else:
            _emit_publish_fail_metrics(
                metrics_queue=metrics_queue,
                svc_counter=svc_counter,
                last_exc=last_exc,
                batch=batch,
            )

        _emit_queue_wait_metrics(
            metrics_queue=metrics_queue,
            batch=batch,
            publish_start_ms=publish_start_ms,
        )

        last_queue_warn_ts = _log_queue_backlog(
            worker_id=worker_id,
            publish_queue=publish_queue,
            last_queue_warn_ts=last_queue_warn_ts,
        )


def run_publisher_workers(
    publish_queue: Any,
    metrics_queue: Any | None,
    stop_event: Any | None,
) -> None:
    workers = max(PUBLISHER_SETTINGS.workers, 1)
    threads = []
    for i in range(workers):
        t = threading.Thread(
            target=publisher_worker,
            args=(i, publish_queue, metrics_queue, stop_event),
            name=f"publisher-worker-{i}",
            daemon=True,
        )
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    if stop_event is not None and stop_event.is_set():
        drain_timeout = max(SIMULATOR_SETTINGS.shutdown_drain_timeout_sec, 0.0)
        _drain_publish_queue(
            publish_queue=publish_queue,
            metrics_queue=metrics_queue,
            stop_event=stop_event,
            drain_timeout_sec=drain_timeout,
        )

    flush_timeout = max(SIMULATOR_SETTINGS.shutdown_drain_timeout_sec, 5.0)
    close_producer(flush_timeout)


def _drain_publish_queue(
    publish_queue: Any,
    metrics_queue: Any | None,
    stop_event: Any | None,
    drain_timeout_sec: float,
) -> None:
    if drain_timeout_sec <= 0:
        return

    deadline = time.perf_counter() + drain_timeout_sec
    drained = 0
    while time.perf_counter() < deadline:
        try:
            batch = publish_queue.get(timeout=0.2)
        except std_queue.Empty:
            if stop_event is not None and stop_event.is_set():
                break
            continue

        if batch is None:
            break
        if not batch:
            continue

        svc_counter = Counter(message.service for message in batch)
        publish_start_ms = int(time.time() * 1000)
        send_duration_ms, send_ok, last_exc = _send_batch(
            batch=batch,
            svc_counter=svc_counter,
            metrics_queue=metrics_queue,
            retry_max=max(PUBLISHER_SETTINGS.retry_max, 0),
            retry_backoff_sec=max(PUBLISHER_SETTINGS.retry_backoff_sec, 0.0),
        )
        if send_ok:
            _emit_publish_metrics(
                metrics_queue=metrics_queue,
                svc_counter=svc_counter,
                send_duration_ms=send_duration_ms,
            )
        else:
            _emit_publish_fail_metrics(
                metrics_queue=metrics_queue,
                svc_counter=svc_counter,
                last_exc=last_exc,
                batch=batch,
            )
        _emit_queue_wait_metrics(
            metrics_queue=metrics_queue,
            batch=batch,
            publish_start_ms=publish_start_ms,
        )
        drained += len(batch)

    if drained > 0:
        _logger.info("[publisher] drained messages=%d before shutdown", drained)


def _send_batch(
    batch: list[Any],
    svc_counter: Counter,
    metrics_queue: Any | None,
    retry_max: int,
    retry_backoff_sec: float,
) -> Tuple[int, bool, Exception | None]:
    send_start = time.perf_counter()
    send_ok = False
    last_exc: Exception | None = None

    for attempt in range(retry_max + 1):
        try:
            publish_batch_direct_sync(batch)
            send_ok = True
            break
        except Exception as exc:
            last_exc = exc
            for svc, cnt in svc_counter.items():
                emit_metric(metrics_queue, PUBLISH_FAIL_TOTAL, cnt, svc)
            if attempt < retry_max and retry_backoff_sec > 0:
                time.sleep(retry_backoff_sec)

    send_duration_ms = int((time.perf_counter() - send_start) * 1000)
    return send_duration_ms, send_ok, last_exc


def _emit_publish_metrics(
    metrics_queue: Any | None,
    svc_counter: Counter,
    send_duration_ms: int,
) -> None:
    for svc, cnt in svc_counter.items():
        emit_metric(metrics_queue, PUBLISHED_TOTAL, cnt, svc)
        emit_metric(metrics_queue, PUBLISH_LATENCY_SUM_MS, send_duration_ms * cnt, svc)
        emit_metric(metrics_queue, PUBLISH_LATENCY_COUNT, cnt, svc)


def _emit_publish_fail_metrics(
    metrics_queue: Any | None,
    svc_counter: Counter,
    last_exc: Exception | None,
    batch: list[Any],
) -> None:
    if last_exc is not None:
        publish_dlq_batch_sync(batch, last_exc)
    for svc, cnt in svc_counter.items():
        emit_metric(metrics_queue, PUBLISH_FAIL_DROP_TOTAL, cnt, svc)


def _emit_queue_wait_metrics(
    metrics_queue: Any | None,
    batch: list[Any],
    publish_start_ms: int,
) -> None:
    wait_sum_by_service: Dict[str, int] = defaultdict(int)
    wait_count_by_service: Dict[str, int] = defaultdict(int)
    for message in batch:
        enqueued_ms = getattr(message, "enqueued_ms", None)
        if enqueued_ms is None:
            continue
        wait_ms = publish_start_ms - enqueued_ms
        if wait_ms < 0:
            wait_ms = 0
        wait_sum_by_service[message.service] += wait_ms
        wait_count_by_service[message.service] += 1
    for svc, total_ms in wait_sum_by_service.items():
        emit_metric(metrics_queue, QUEUE_WAIT_SUM_MS, total_ms, svc)
    for svc, cnt in wait_count_by_service.items():
        emit_metric(metrics_queue, QUEUE_WAIT_COUNT, cnt, svc)


def _log_queue_backlog(
    worker_id: int,
    publish_queue: Any,
    last_queue_warn_ts: float,
) -> float:
    now = time.perf_counter()
    if (now - last_queue_warn_ts) < 5.0:
        return last_queue_warn_ts
    try:
        queue_depth = queue_qsize(publish_queue)
        queue_capacity = queue_maxsize(publish_queue, fallback=SIMULATOR_SETTINGS.queue_size)
        if queue_capacity:
            fill_ratio = queue_depth / queue_capacity
            if fill_ratio >= PUBLISHER_SETTINGS.queue_warn_ratio:
                _logger.info(
                    "[publisher] queue backlog worker=%d queue=%d/%d (%.0f%%)",
                    worker_id,
                    queue_depth,
                    queue_capacity,
                    fill_ratio * 100,
                )
    except Exception:
        return last_queue_warn_ts
    return now
