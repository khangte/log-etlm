# -----------------------------------------------------------------------------
# 파일명 : log_gateway/simulator_pipeline.py
# 목적   : 서비스별 시뮬레이터 배치 생성 루프를 구성하고 큐에 로그를 적재
# -----------------------------------------------------------------------------

from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Any, Dict, List, Tuple

from .config.timeband import current_hour_kst, pick_multiplier

# 서비스 루프 기본 설정
# 10k RPS = (서비스 4개 × 루프당 625 RPS × 배치 100건)
LOG_BATCH_SIZE: int = int(os.getenv("LOG_BATCH_SIZE", "100"))
QUEUE_SIZE: int = int(os.getenv("QUEUE_SIZE", "10000"))
LOOPS_PER_SERVICE: int = int(os.getenv("LOOPS_PER_SERVICE", "8"))
QUEUE_WARN_RATIO: float = float(os.getenv("QUEUE_WARN_RATIO", "0.8"))
QUEUE_LOW_WATERMARK_RATIO: float = float(os.getenv("QUEUE_LOW_WATERMARK_RATIO", "0.2"))
QUEUE_LOW_SLEEP_SCALE: float = float(os.getenv("QUEUE_LOW_SLEEP_SCALE", "0.3"))
QUEUE_THROTTLE_RATIO: float = float(os.getenv("QUEUE_THROTTLE_RATIO", "0.9"))
QUEUE_RESUME_RATIO: float = float(os.getenv("QUEUE_RESUME_RATIO", "0.75"))
QUEUE_THROTTLE_SLEEP: float = float(os.getenv("QUEUE_THROTTLE_SLEEP", "0.05"))
QUEUE_SOFT_THROTTLE_RATIO: float = float(os.getenv("QUEUE_SOFT_THROTTLE_RATIO", "0.85"))
QUEUE_SOFT_RESUME_RATIO: float = float(os.getenv("QUEUE_SOFT_RESUME_RATIO", "0.7"))
QUEUE_SOFT_SCALE_STEP: float = float(os.getenv("QUEUE_SOFT_SCALE_STEP", "0.1"))
QUEUE_SOFT_SCALE_MIN: float = float(os.getenv("QUEUE_SOFT_SCALE_MIN", "0.2"))
QUEUE_SOFT_SCALE_MAX: float = float(os.getenv("QUEUE_SOFT_SCALE_MAX", "1.0"))

_logger = logging.getLogger("log_gateway.simulator_pipeline")
_logger.setLevel(logging.INFO)
if not _logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    )
    _logger.addHandler(handler)


async def _service_stream_loop(
    service: str,
    simulator: Any,
    target_rps: float,
    publish_queue: "asyncio.Queue[Tuple[str, str, bool]]",
    bands: List[Any],
    weight_mode: str,
    log_batch_size: int,
) -> None:
    """서비스별로 배치 로그를 생성해 퍼블리시 큐에 쌓는다."""
    throttle_scale = QUEUE_SOFT_SCALE_MAX
    while True:
        loop_start = time.perf_counter()
        hour = current_hour_kst()
        multiplier = pick_multiplier(bands, hour_kst=hour, mode=weight_mode) if bands else 1.0
        effective_rps = max(target_rps * multiplier, 0.01)
        scaled_rps = max(effective_rps * throttle_scale, 0.01)
        batch_size = log_batch_size

        logs = simulator.generate_logs(batch_size)
        enqueue_start = time.perf_counter()
        for log in logs:
            payload = simulator.render(log)
            await publish_queue.put((service, payload, log.get("level") == "ERROR"))

        enqueue_duration = time.perf_counter() - enqueue_start

        desired_period = batch_size / scaled_rps
        elapsed = time.perf_counter() - loop_start
        sleep_time = max(0.0, desired_period - elapsed)

        queue_depth = publish_queue.qsize()
        queue_capacity = publish_queue.maxsize

        if elapsed >= desired_period:
            _logger.info(
                "[simulator] behind target service=%s target_rps=%.1f batch=%d "
                "duration=%.4fs target_interval=%.4fs enqueue=%.4fs queue=%d",
                service,
                effective_rps,
                batch_size,
                elapsed,
                desired_period,
                enqueue_duration,
                queue_depth,
            )

        if queue_capacity and queue_capacity > 0:
            fill_ratio = queue_depth / queue_capacity
            if fill_ratio >= QUEUE_THROTTLE_RATIO:
                throttle_scale = QUEUE_SOFT_SCALE_MIN
                throttle_started_at = time.perf_counter()
                _logger.info(
                    "[simulator] throttling service=%s queue=%d/%d (%.0f%%)",
                    service,
                    queue_depth,
                    queue_capacity,
                    fill_ratio * 100,
                )
                
                while True:
                    await asyncio.sleep(QUEUE_THROTTLE_SLEEP)
                    queue_depth = publish_queue.qsize()
                    fill_ratio = queue_depth / queue_capacity
                    if fill_ratio <= QUEUE_RESUME_RATIO:
                        _logger.info(
                            "[simulator] throttle release service=%s queue=%d/%d (%.0f%%)",
                            service,
                            queue_depth,
                            queue_capacity,
                            fill_ratio * 100,
                        )
                        throttle_duration = time.perf_counter() - throttle_started_at
                        _logger.info(
                            "[simulator] throttle duration service=%s duration=%.3fs",
                            service,
                            throttle_duration,
                        )
                        break
                continue
            if fill_ratio >= QUEUE_SOFT_THROTTLE_RATIO:
                new_scale = max(QUEUE_SOFT_SCALE_MIN, throttle_scale - QUEUE_SOFT_SCALE_STEP)
                if new_scale < throttle_scale:
                    throttle_scale = new_scale
                    _logger.info(
                        "[simulator] soft throttle service=%s scale=%.2f queue=%d/%d (%.0f%%)",
                        service,
                        throttle_scale,
                        queue_depth,
                        queue_capacity,
                        fill_ratio * 100,
                    )
            elif fill_ratio <= QUEUE_SOFT_RESUME_RATIO:
                new_scale = min(QUEUE_SOFT_SCALE_MAX, throttle_scale + QUEUE_SOFT_SCALE_STEP)
                if new_scale > throttle_scale:
                    throttle_scale = new_scale
                    _logger.info(
                        "[simulator] soft throttle release service=%s scale=%.2f queue=%d/%d (%.0f%%)",
                        service,
                        throttle_scale,
                        queue_depth,
                        queue_capacity,
                        fill_ratio * 100,
                    )
            if sleep_time > 0 and fill_ratio <= QUEUE_LOW_WATERMARK_RATIO:
                sleep_time *= QUEUE_LOW_SLEEP_SCALE
            if fill_ratio >= QUEUE_WARN_RATIO:
                _logger.info(
                    "[simulator] queue backlog service=%s queue=%d/%d (%.0f%%)",
                    service,
                    queue_depth,
                    queue_capacity,
                    fill_ratio * 100,
                )

        if sleep_time > 0:
            await asyncio.sleep(sleep_time)


def create_service_tasks(
    simulators: Dict[str, Any],
    base_rps: float,
    service_rps: Dict[str, float],
    bands: List[Any],
    weight_mode: str,
    log_batch_size: int = LOG_BATCH_SIZE,
    queue_size: int = QUEUE_SIZE,
    loops_per_service: int = LOOPS_PER_SERVICE,
) -> Tuple["asyncio.Queue[Tuple[str, str, bool]]", List[asyncio.Task], List[str]]:
    """시뮬레이터 태스크들을 생성하고 publish 큐와 함께 반환."""
    publish_queue: "asyncio.Queue[Tuple[str, str, bool]]" = asyncio.Queue(maxsize=queue_size)

    available_services = list(simulators.keys())
    service_count = max(len(available_services), 1)
    fallback_rps = base_rps / service_count

    loops = max(loops_per_service, 1)
    service_tasks: List[asyncio.Task] = []
    for service in available_services:
        target = service_rps.get(service, fallback_rps)
        per_loop_rps = target / loops
        for idx in range(loops):
            task = asyncio.create_task(
                _service_stream_loop(
                    service=service,
                    simulator=simulators[service],
                    target_rps=per_loop_rps,
                    publish_queue=publish_queue,
                    bands=bands,
                    weight_mode=weight_mode,
                    log_batch_size=log_batch_size,
                ),
                name=f"service-loop-{service}-{idx}",
            )
            service_tasks.append(task)

    return publish_queue, service_tasks, available_services
