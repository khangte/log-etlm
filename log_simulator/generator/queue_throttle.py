# -----------------------------------------------------------------------------
# 파일명 : log_simulator/generator/queue_throttle.py
# 목적   : 큐 수위 기반 스로틀/슬립 로직 분리
# -----------------------------------------------------------------------------

from __future__ import annotations

import asyncio
import time
from typing import Any, Tuple

from ..queue.queue_adapter import queue_qsize


async def apply_queue_throttle(
    *,
    publish_queue: Any,
    queue_depth: int,
    queue_capacity: int,
    throttle_scale: float,
    sleep_time: float,
    stop_event: Any | None,
    logger: Any,
    service: str | None,
    queue_throttle_ratio: float,
    queue_resume_ratio: float,
    queue_throttle_sleep: float,
    queue_soft_throttle_ratio: float,
    queue_soft_resume_ratio: float,
    queue_soft_scale_step: float,
    queue_soft_scale_min: float,
    queue_soft_scale_max: float,
    queue_low_watermark_ratio: float,
    queue_low_sleep_scale: float,
    queue_warn_ratio: float,
) -> Tuple[float, float, str]:
    """큐 수위에 따라 스로틀 스케일/슬립 시간을 조정한다."""
    if not queue_capacity or queue_capacity <= 0:
        return throttle_scale, sleep_time, "ok"

    fill_ratio = queue_depth / queue_capacity
    if fill_ratio >= queue_throttle_ratio:
        throttle_scale = queue_soft_scale_min
        throttle_started_at = time.perf_counter()
        if service:
            logger.info(
                "[simulator] throttling service=%s queue=%d/%d (%.0f%%)",
                service,
                queue_depth,
                queue_capacity,
                fill_ratio * 100,
            )
        else:
            logger.info(
                "[simulator] throttling queue=%d/%d (%.0f%%)",
                queue_depth,
                queue_capacity,
                fill_ratio * 100,
            )

        while True:
            if stop_event is not None and stop_event.is_set():
                return throttle_scale, sleep_time, "exit"
            await asyncio.sleep(queue_throttle_sleep)
            queue_depth = queue_qsize(publish_queue)
            fill_ratio = queue_depth / queue_capacity
            if fill_ratio <= queue_resume_ratio:
                if service:
                    logger.info(
                        "[simulator] throttle release service=%s queue=%d/%d (%.0f%%)",
                        service,
                        queue_depth,
                        queue_capacity,
                        fill_ratio * 100,
                    )
                else:
                    logger.info(
                        "[simulator] throttle release queue=%d/%d (%.0f%%)",
                        queue_depth,
                        queue_capacity,
                        fill_ratio * 100,
                    )
                throttle_duration = time.perf_counter() - throttle_started_at
                if service:
                    logger.info(
                        "[simulator] throttle duration service=%s duration=%.3fs",
                        service,
                        throttle_duration,
                    )
                else:
                    logger.info(
                        "[simulator] throttle duration duration=%.3fs",
                        throttle_duration,
                    )
                break
        return throttle_scale, sleep_time, "continue"

    if fill_ratio >= queue_soft_throttle_ratio:
        new_scale = max(queue_soft_scale_min, throttle_scale - queue_soft_scale_step)
        if new_scale < throttle_scale:
            throttle_scale = new_scale
            if service:
                logger.info(
                    "[simulator] soft throttle service=%s scale=%.2f queue=%d/%d (%.0f%%)",
                    service,
                    throttle_scale,
                    queue_depth,
                    queue_capacity,
                    fill_ratio * 100,
                )
            else:
                logger.info(
                    "[simulator] soft throttle scale=%.2f queue=%d/%d (%.0f%%)",
                    throttle_scale,
                    queue_depth,
                    queue_capacity,
                    fill_ratio * 100,
                )
    elif fill_ratio <= queue_soft_resume_ratio:
        new_scale = min(queue_soft_scale_max, throttle_scale + queue_soft_scale_step)
        if new_scale > throttle_scale:
            throttle_scale = new_scale
            if service:
                logger.info(
                    "[simulator] soft throttle release service=%s scale=%.2f queue=%d/%d (%.0f%%)",
                    service,
                    throttle_scale,
                    queue_depth,
                    queue_capacity,
                    fill_ratio * 100,
                )
            else:
                logger.info(
                    "[simulator] soft throttle release scale=%.2f queue=%d/%d (%.0f%%)",
                    throttle_scale,
                    queue_depth,
                    queue_capacity,
                    fill_ratio * 100,
                )

    if sleep_time > 0 and fill_ratio <= queue_low_watermark_ratio:
        sleep_time *= queue_low_sleep_scale
    if fill_ratio >= queue_warn_ratio:
        if service:
            logger.info(
                "[simulator] queue backlog service=%s queue=%d/%d (%.0f%%)",
                service,
                queue_depth,
                queue_capacity,
                fill_ratio * 100,
            )
        else:
            logger.info(
                "[simulator] queue backlog queue=%d/%d (%.0f%%)",
                queue_depth,
                queue_capacity,
                fill_ratio * 100,
            )

    return throttle_scale, sleep_time, "ok"
