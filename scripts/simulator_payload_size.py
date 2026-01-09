"""시뮬레이터 이벤트(payload) 크기를 샘플링해서 출력."""

from __future__ import annotations

import argparse
import json
import os
import statistics
import time
from pathlib import Path
import sys
from typing import Iterable

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR))

from log_simulator.config.profile_route_settings import load_profile_context
from log_simulator.generator.batch_builder import apply_created_ms, build_batch_items
from log_simulator.simulator.build_simulators import build_simulators

try:
    import orjson
except ImportError:  # pragma: no cover - optional dependency
    orjson = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="시뮬레이터 이벤트 payload(JSON) 크기를 바이트로 측정합니다."
    )
    parser.add_argument(
        "--per-service",
        type=int,
        default=200,
        help="서비스별 샘플 이벤트 수(기본값 200)",
    )
    parser.add_argument(
        "--event-mode",
        default=os.getenv("SIM_EVENT_MODE", "domain"),
        help="event_mode override (기본값: 환경변수 SIM_EVENT_MODE)",
    )
    return parser.parse_args()


def _to_bytes(value: dict) -> bytes:
    if orjson is not None:
        return orjson.dumps(value)
    return json.dumps(value, ensure_ascii=False).encode("utf-8")


def _summarize(service: str, sizes: Iterable[int]) -> None:
    values = list(sizes)
    if not values:
        print(f"{service}: 샘플 없음")
        return
    avg = statistics.fmean(values)
    print(
        f"{service}: count={len(values)} avg={avg:.1f}B "
        f"min={min(values)}B max={max(values)}B"
    )


def main() -> None:
    args = parse_args()
    context = load_profile_context()
    context.profile["event_mode"] = args.event_mode
    simulators = build_simulators(context.profile)

    created_ms = int(time.time() * 1000)
    enqueued_ms = int(time.time() * 1000)

    all_sizes: list[int] = []
    for service, simulator in simulators.items():
        events = simulator.generate_events(max(args.per_service, 1))
        apply_created_ms(events, created_ms)
        batch = build_batch_items(service, events, enqueued_ms)
        sizes = [len(_to_bytes(item.value)) for item in batch]
        _summarize(service, sizes)
        all_sizes.extend(sizes)

    if all_sizes:
        avg = statistics.fmean(all_sizes)
        print(f"overall: count={len(all_sizes)} avg={avg:.1f}B")


if __name__ == "__main__":
    main()
