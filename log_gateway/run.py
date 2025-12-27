# -----------------------------------------------------------------------------
# 파일명 : log_gateway/run.py
# 목적   : 로그 시뮬레이터 파이프라인 실행(run_simulator) 진입점
# 설명   : generator.build_generation_pipeline() 결과를 실행하고 stats 리포터를 붙임
# -----------------------------------------------------------------------------

from __future__ import annotations

import asyncio
from typing import Optional

from .config.stats import stats_reporter
from .generator import PROFILE_NAME, build_generation_pipeline


async def run_simulator(profile_name: Optional[str] = None) -> None:
    """
    시뮬레이터 파이프라인을 실행한다.

    Args:
        profile_name: 사용할 프로파일 이름. None이면 기본값 사용.
    """
    (
        services,
        publish_queue,
        stats_queue,
        service_tasks,
        publisher_tasks,
    ) = build_generation_pipeline(profile_name or PROFILE_NAME)

    # stats 리포터는 백그라운드 태스크로 붙여 throughput 로그를 남긴다.
    stats_task = asyncio.create_task(
        stats_reporter(stats_queue=stats_queue, services=services),
        name="stats-reporter",
    )

    tasks = service_tasks + publisher_tasks + [stats_task]
    try:
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            task.cancel()
