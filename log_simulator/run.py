# -----------------------------------------------------------------------------
# file: log_simulator/run.py
# purpose: run generator/publisher/metrics as multiprocessing processes
# -----------------------------------------------------------------------------

from __future__ import annotations

import asyncio
import logging
import multiprocessing as mp
import os
import signal
import time
from typing import Any, Callable

from .config.settings import SIMULATOR_SETTINGS
from .generator.generator_runner import run_generator_async
from .publisher.metrics_reporter import run_metrics_reporter
from .publisher.publisher_runner import run_publisher_workers


_logger = logging.getLogger("log_simulator.run")
_logger.setLevel(logging.INFO)
if not _logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    )
    _logger.addHandler(handler)


def _run_guarded(name: str, target: Callable[..., Any], *args: Any) -> None:
    """프로세스 타깃을 감싸 예외를 로깅하고 중지 신호를 전파한다."""
    try:
        target(*args)
    except Exception:
        _logger.exception("process crashed name=%s", name)
        if args and hasattr(args[-1], "set"):
            try:
                args[-1].set()
            except Exception:
                pass


def _run_generator(
    publish_queue: Any,
    metrics_queue: Any,
    stop_event: Any,
) -> None:
    """제너레이터 루프를 이벤트 루프에서 실행한다."""
    asyncio.run(run_generator_async(publish_queue, metrics_queue, stop_event))


def main() -> None:
    """generator/publisher/metrics 프로세스를 실행하고 감시한다."""
    start_method = os.getenv("SIM_MP_START_METHOD", "spawn")
    ctx = mp.get_context(start_method)

    publish_queue = ctx.Queue(maxsize=SIMULATOR_SETTINGS.queue_size)
    metrics_queue = ctx.SimpleQueue()
    stop_event = ctx.Event()

    def _handle_signal(signum, _frame) -> None:
        """종료 신호를 받아 stop_event를 설정한다."""
        _logger.info("signal received=%s; stopping", signum)
        stop_event.set()

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    processes = [
        ctx.Process(
            target=_run_guarded,
            name="generator-process",
            args=("generator-process", _run_generator, publish_queue, metrics_queue, stop_event),
        ),
        ctx.Process(
            target=_run_guarded,
            name="publisher-process",
            args=("publisher-process", run_publisher_workers, publish_queue, metrics_queue, stop_event),
        ),
        ctx.Process(
            target=_run_guarded,
            name="metrics-process",
            args=("metrics-process", run_metrics_reporter, metrics_queue, stop_event),
        ),
    ]

    for proc in processes:
        proc.start()

    try:
        while not stop_event.is_set():
            all_dead = True
            for proc in processes:
                if proc.is_alive():
                    all_dead = False
                    continue
                if proc.exitcode not in (0, None):
                    _logger.warning(
                        "process exited name=%s code=%s",
                        proc.name,
                        proc.exitcode,
                    )
                    stop_event.set()
                proc.join(timeout=0)
            if all_dead:
                break
            time.sleep(0.1)
    finally:
        stop_event.set()
        try:
            metrics_queue.put(None)
        except Exception:
            pass
        alive = []
        timeout = max(SIMULATOR_SETTINGS.shutdown_drain_timeout_sec, 1.0)
        for proc in processes:
            proc.join(timeout=timeout)
            if proc.is_alive():
                alive.append(proc.name)
        if alive:
            _logger.warning("processes still alive after stop: %s", ", ".join(alive))


if __name__ == "__main__":
    main()
