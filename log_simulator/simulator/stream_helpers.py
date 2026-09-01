# -----------------------------------------------------------------------------
# 파일명 : log_simulator/simulator/stream_helpers.py
# 목적   : 시뮬레이터 스트림 루프 헬퍼 모음
# -----------------------------------------------------------------------------

from __future__ import annotations

import asyncio
import time
from typing import Any

from ..models.messages import BatchMessage
from .settings import SimulatorSettings


def _is_error_event(ev: dict) -> bool:
    """is_error_event 처리를 수행한다."""
    r = ev.get("result")
    if r is not None:
        return r == "fail"
    sc = ev.get("status_code")
    if isinstance(sc, int):
        return sc >= 500
    return False


def build_batch_messages_from_simulator(
    simulator: Any,
    service: str,
    count: int,
) -> list[BatchMessage]:
    """build_batch_messages_from_simulator 처리를 수행한다."""
    batch_items: list[BatchMessage] = []
    append = batch_items.append
    render = simulator.render_bytes
    for _ in range(count):
        events = simulator.generate_events_one()
        for ev in events:
            payload = render(ev)
            request_id = ev.get("request_id")
            key = str(request_id).encode("utf-8") if request_id else None
            append(
                BatchMessage(
                    service=service,
                    value=payload,
                    key=key,
                    replicate_error=_is_error_event(ev),
                )
            )
    return batch_items


def log_behind(
    logger,
    *,
    service: str,
    effective_eps: float,
    batch_size: int,
    elapsed: float,
    desired_period: float,
    enqueue_duration: float,
    queue_depth: int,
    loop_start: float,
    last_behind_log_ts: float,
    behind_log_every_sec: float,
) -> float:
    """log_behind 처리를 수행한다."""
    if elapsed < desired_period:
        return last_behind_log_ts

    should_log = True
    if behind_log_every_sec > 0:
        should_log = (loop_start - last_behind_log_ts) >= behind_log_every_sec
    if should_log:
        logger.info(
            "[simulator] behind target service=%s target_eps=%.1f batch=%d "
            "duration=%.4fs target_interval=%.4fs enqueue=%.4fs queue=%d",
            service,
            effective_eps,
            batch_size,
            elapsed,
            desired_period,
            enqueue_duration,
            queue_depth,
        )
        return loop_start
    return last_behind_log_ts


async def apply_queue_backpressure(
    logger,
    *,
    settings: SimulatorSettings,
    service: str,
    publish_queue: "asyncio.Queue[list[BatchMessage]]",
    throttle_scale: float,
    sleep_time: float,
    queue_depth: int,
    queue_capacity: int,
) -> tuple[float, float, bool]:
    """apply_queue_backpressure 처리를 수행한다."""
    if not queue_capacity or queue_capacity <= 0:
        return throttle_scale, sleep_time, False

    fill_ratio = queue_depth / queue_capacity
    if fill_ratio >= settings.queue_throttle_ratio:
        throttle_scale = settings.queue_soft_scale_min
        throttle_started_at = time.perf_counter()
        logger.info(
            "[simulator] throttling service=%s queue=%d/%d (%.0f%%)",
            service,
            queue_depth,
            queue_capacity,
            fill_ratio * 100,
        )
        while True:
            await asyncio.sleep(settings.queue_throttle_sleep)
            queue_depth = publish_queue.qsize()
            fill_ratio = queue_depth / queue_capacity
            if fill_ratio <= settings.queue_resume_ratio:
                logger.info(
                    "[simulator] throttle release service=%s queue=%d/%d (%.0f%%)",
                    service,
                    queue_depth,
                    queue_capacity,
                    fill_ratio * 100,
                )
                throttle_duration = time.perf_counter() - throttle_started_at
                logger.info(
                    "[simulator] throttle duration service=%s duration=%.3fs",
                    service,
                    throttle_duration,
                )
                break
        return throttle_scale, sleep_time, True

    if fill_ratio >= settings.queue_soft_throttle_ratio:
        new_scale = max(
            settings.queue_soft_scale_min,
            throttle_scale - settings.queue_soft_scale_step,
        )
        if new_scale < throttle_scale:
            throttle_scale = new_scale
            logger.info(
                "[simulator] soft throttle service=%s scale=%.2f queue=%d/%d (%.0f%%)",
                service,
                throttle_scale,
                queue_depth,
                queue_capacity,
                fill_ratio * 100,
            )
    elif fill_ratio <= settings.queue_soft_resume_ratio:
        new_scale = min(
            settings.queue_soft_scale_max,
            throttle_scale + settings.queue_soft_scale_step,
        )
        if new_scale > throttle_scale:
            throttle_scale = new_scale
            logger.info(
                "[simulator] soft throttle release service=%s scale=%.2f queue=%d/%d (%.0f%%)",
                service,
                throttle_scale,
                queue_depth,
                queue_capacity,
                fill_ratio * 100,
            )

    if fill_ratio >= settings.queue_warn_ratio:
        logger.info(
            "[simulator] queue backlog service=%s queue=%d/%d (%.0f%%)",
            service,
            queue_depth,
            queue_capacity,
            fill_ratio * 100,
        )
    return throttle_scale, sleep_time, False
