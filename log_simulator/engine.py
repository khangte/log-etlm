# -----------------------------------------------------------------------------
# 파일명 : log_simulator/engine.py
# 목적   : 로그 시뮬레이터 엔진 시작/중지 관리
# 설명   : pipeline_builder.assemble_pipeline()로 조립하고 stats 리포터를 붙임
# -----------------------------------------------------------------------------

from __future__ import annotations

import asyncio
import logging

from .config.profile_route_settings import load_profile_context
from .simulator.settings import SIMULATOR_SETTINGS
from .config.stats import stats_reporter
from .pipeline_builder import Pipeline, assemble_pipeline
from .producer.client import close_producer
from .simulator.eps_policy import allocate_service_eps
from .simulator.simulators_builder import build_simulators


logger = logging.getLogger("log_simulator.engine")


class SimulatorEngine:
    def __init__(self) -> None:
        self._started = False
        self._tasks: list[asyncio.Task] = []
        self._pipe: Pipeline | None = None
        self._stats_task: asyncio.Task | None = None

    async def start(self) -> None:
        """start 처리를 수행한다."""
        if self._started:
            return

        # 프로파일/정책을 읽고 파이프라인을 한 번에 조립한다.
        context = load_profile_context()
        context.profile["event_mode"] = SIMULATOR_SETTINGS.event_mode
        simulators = build_simulators(context.profile)
        base_eps, service_eps = allocate_service_eps(
            total_eps=context.total_eps,
            mix=context.mix,
            services=list(simulators.keys()),
            simulator_share=SIMULATOR_SETTINGS.simulator_share,
        )
        pipe = assemble_pipeline(
            simulators=simulators,
            base_eps=base_eps,
            service_eps=service_eps,
            bands=context.bands,
        )

        # stats 리포터는 백그라운드 태스크로 붙여 throughput 로그를 남긴다.
        stats_task = asyncio.create_task(
            stats_reporter(stats_queue=pipe.stats_queue, services=list(simulators.keys())),
            name="stats-reporter",
        )

        self._tasks = pipe.service_tasks + pipe.publisher_tasks + [stats_task]
        self._pipe = pipe
        self._stats_task = stats_task
        self._started = True

    async def wait(self) -> None:
        """wait 처리를 수행한다."""
        if not self._tasks:
            return
        await asyncio.gather(*self._tasks)

    async def stop(self) -> None:
        """stop 처리를 수행한다."""
        if not self._started:
            return
        if self._pipe:
            # 종료 순서: 생성 중단 → 큐 드레인 → 퍼블리셔 중단.
            for task in self._pipe.service_tasks:
                task.cancel()
            await asyncio.gather(*self._pipe.service_tasks, return_exceptions=True)

            try:
                await asyncio.wait_for(
                    self._pipe.publish_queue.join(),
                    timeout=SIMULATOR_SETTINGS.shutdown_drain_timeout_sec,
                )
            except asyncio.TimeoutError:
                logger.warning("publish queue drain timeout; proceeding with shutdown")

            for task in self._pipe.publisher_tasks:
                task.cancel()
            await asyncio.gather(*self._pipe.publisher_tasks, return_exceptions=True)

        if self._stats_task:
            self._stats_task.cancel()
            await asyncio.gather(self._stats_task, return_exceptions=True)
        close_producer(5)
        self._tasks = []
        self._pipe = None
        self._stats_task = None
        self._started = False


engine = SimulatorEngine()


async def run_simulator() -> None:
    """시뮬레이터 파이프라인을 실행한다."""
    await engine.start()
    try:
        await engine.wait()
    finally:
        await engine.stop()
