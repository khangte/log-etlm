# -----------------------------------------------------------------------------
# 파일명 : log_simulator/simulator/stream_helpers.py
# 목적   : 시뮬레이터 스트림 루프 헬퍼 모음
# -----------------------------------------------------------------------------

from __future__ import annotations

import asyncio
import time
from typing import Any

from ..models.messages import BatchMessage
from .settings import QueueThrottleConfig

def adjust_eps_for_domain_rate(simulator: Any, eps: float) -> float:
    """도메인 이벤트 비율을 고려해 EPS를 보정한다."""
    domain_rate = float(getattr(simulator, "domain_event_rate", 1.0))
    if domain_rate <= 0:
        domain_rate = 1.0
    return max(eps / max(domain_rate, 0.01), 0.01)


def _is_error_event(ev: dict) -> bool:
    """이벤트가 오류로 간주되는지 여부를 반환한다."""
    r = ev.get("result")
    if r is not None:
        return r == "fail"
    sc = ev.get("status_code")
    if isinstance(sc, int):
        return sc >= 500
    return False


def build_batch_messages(
    simulator: Any,
    service: str,
    events: list[dict],
) -> list[BatchMessage]:
    """시뮬레이터 이벤트 목록을 BatchMessage로 변환한다."""
    batch_items: list[BatchMessage] = []
    for ev in events:
        payload = simulator.render_bytes(ev)
        request_id = ev.get("request_id")
        key = str(request_id).encode("utf-8") if request_id else None
        batch_items.append(
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
    """지연 발생 시 로그를 남기고 마지막 로그 시점을 갱신한다."""
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


def _apply_soft_throttle(
    logger,
    config: QueueThrottleConfig,
    *,
    service: str,
    throttle_scale: float,
    queue_depth: int,
    queue_capacity: int,
    fill_ratio: float,
) -> float:
    """소프트 스로틀 규칙에 따라 throttle 비율을 조정한다."""
    if fill_ratio >= config.soft_throttle_ratio:
        new_scale = max(config.soft_scale_min, throttle_scale - config.soft_scale_step)
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
    elif fill_ratio <= config.soft_resume_ratio:
        new_scale = min(config.soft_scale_max, throttle_scale + config.soft_scale_step)
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
    return throttle_scale


async def apply_queue_backpressure(
    logger,
    *,
    config: QueueThrottleConfig,
    service: str,
    publish_queue: "asyncio.Queue[list[BatchMessage]]",
    throttle_scale: float,
    sleep_time: float,
    queue_depth: int,
    queue_capacity: int,
) -> tuple[float, float, bool]:
    """큐 백프레셔를 적용하고 (throttle_scale, sleep_time, throttled)를 반환한다."""
    if not queue_capacity or queue_capacity <= 0:
        return throttle_scale, sleep_time, False

    fill_ratio = queue_depth / queue_capacity
    if fill_ratio >= config.throttle_ratio:
        throttle_scale = config.soft_scale_min
        throttle_started_at = time.perf_counter()
        logger.info(
            "[simulator] throttling service=%s queue=%d/%d (%.0f%%)",
            service,
            queue_depth,
            queue_capacity,
            fill_ratio * 100,
        )
        while True:
            await asyncio.sleep(config.throttle_sleep)
            queue_depth = publish_queue.qsize()
            fill_ratio = queue_depth / queue_capacity
            if fill_ratio <= config.resume_ratio:
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

    throttle_scale = _apply_soft_throttle(
        logger,
        config,
        service=service,
        throttle_scale=throttle_scale,
        queue_depth=queue_depth,
        queue_capacity=queue_capacity,
        fill_ratio=fill_ratio,
    )

    if sleep_time > 0 and fill_ratio <= config.low_watermark_ratio:
        sleep_time *= config.low_sleep_scale
    if fill_ratio >= config.warn_ratio:
        logger.info(
            "[simulator] queue backlog service=%s queue=%d/%d (%.0f%%)",
            service,
            queue_depth,
            queue_capacity,
            fill_ratio * 100,
        )
    return throttle_scale, sleep_time, False
