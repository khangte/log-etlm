# -----------------------------------------------------------------------------
# 파일명 : log_simulator/config/stats.py
# 목적   : generator에서 사용하는 통계 출력(terminal logging) 헬퍼
# 설명   : stats_queue를 읽어 일정 주기마다 EPS/서비스별 건수 로그를 남김
# -----------------------------------------------------------------------------
from __future__ import annotations

import asyncio
import logging
import os
from typing import List, Tuple


_logger = logging.getLogger("log_simulator.stats")
_logger.setLevel(logging.INFO)

if not _logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    )
    _logger.addHandler(handler)


async def stats_reporter(
    stats_queue: "asyncio.Queue[Tuple[str, int]]",
    services: List[str],
    interval_sec: float = 10.0,
    logger: logging.Logger | None = None,
) -> None:
    """
    stats 큐를 소비해 터미널에 throughput 로그를 출력한다.

    Args:
        stats_queue: publisher가 put_nowait 하는 (service, count) 큐
        services: 보고 대상 서비스 목록
        interval_sec: 통계 집계 주기(초)
        logger: 기본 logger 대체용
    """
    log = logger or _logger

    loop = asyncio.get_running_loop()
    last_report = loop.time()
    total = 0
    per_service = {svc: 0 for svc in services}

    while True:
        now = loop.time()
        elapsed = now - last_report
        if elapsed >= interval_sec:
            eps = total / elapsed if elapsed > 0 else 0.0
            log.info(
                "[stats pid=%d] eps=%.1f window=%.2fs total=%d by_svc=(%s)",
                os.getpid(),
                eps,
                elapsed,
                total,
                ", ".join(f"{svc}:{per_service.get(svc, 0)}" for svc in services),
            )
            total = 0
            per_service = {svc: 0 for svc in services}
            last_report = now

        timeout = max(0.0, interval_sec - (loop.time() - last_report))
        try:
            service, count = await asyncio.wait_for(stats_queue.get(), timeout=timeout)
            # 대상 서비스만 집계해 잡음 로그를 줄인다.
            if service in per_service:
                per_service[service] += count
            total += count
        except asyncio.TimeoutError:
            continue


import time
from collections import defaultdict

_tps_counter = defaultdict(int)
_last_ts = time.time()

def record_tps(service: str):
    global _last_ts
    _tps_counter[service] += 1
    now = time.time()

    if now - _last_ts >= 1:
        total = sum(_tps_counter.values())
        per_service = ", ".join([f"{svc}={_tps_counter[svc]}" for svc in _tps_counter])

        print(f"[TPS] total={total} | {per_service}")

        _tps_counter.clear()
        _last_ts = now
