# -----------------------------------------------------------------------------
# 파일명 : tests/simulator/test_render_bytes.py
# 목적   : render_bytes orjson 교체 후 출력 동일성 및 성능 검증
# 실행   : uv run python tests/simulator/test_render_bytes.py
# -----------------------------------------------------------------------------

from __future__ import annotations

import json
import time
from typing import Any, Dict

import orjson


def render_bytes_old(log: Dict[str, Any]) -> bytes:
    """변경 전: 표준 json.dumps"""
    return json.dumps(log, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def render_bytes_new(log: Dict[str, Any]) -> bytes:
    """변경 후: orjson.dumps"""
    return orjson.dumps(log)


# 실제 시뮬레이터가 생성하는 이벤트와 유사한 샘플
SAMPLES: list[Dict[str, Any]] = [
    {
        "request_id": "abc-123",
        "service": "auth",
        "status_code": 200,
        "ts_ms": 1748000000000,
    },
    {
        "result": "fail",
        "event_id": "xyz-999",
        "msg": "한글 포함 데이터",
    },
    {
        "nested": {"a": 1},
        "list": [1, 2, 3],
        "null_val": None,
        "flag": True,
    },
    {
        "service": "payment",
        "amount": 9900,
        "currency": "KRW",
        "user": "user_42",
    },
]

BENCHMARK_SAMPLE: Dict[str, Any] = {
    "request_id": "abc-123",
    "service": "auth",
    "status_code": 200,
    "ts_ms": 1748000000000,
    "event_id": "evt-xyz",
    "result": "ok",
}

BENCHMARK_N = 100_000


def test_output_identical() -> bool:
    """orjson 출력이 표준 json과 바이트 단위로 동일한지 검증한다."""
    print("=== 출력 동일성 검증 ===")
    all_ok = True
    for data in SAMPLES:
        old = render_bytes_old(data)
        new = render_bytes_new(data)
        match = old == new
        status = "✅" if match else "❌"
        print(f"  {status} {list(data.keys())[:2]}... match={match}")
        if not match:
            all_ok = False
            print(f"     old: {old}")
            print(f"     new: {new}")
    return all_ok


def test_benchmark() -> dict[str, float]:
    """표준 json 대비 orjson 속도를 측정하고 결과를 반환한다."""
    print(f"\n=== 속도 비교 ({BENCHMARK_N:,}회 반복) ===")

    t0 = time.perf_counter()
    for _ in range(BENCHMARK_N):
        render_bytes_old(BENCHMARK_SAMPLE)
    t_old = time.perf_counter() - t0

    t0 = time.perf_counter()
    for _ in range(BENCHMARK_N):
        render_bytes_new(BENCHMARK_SAMPLE)
    t_new = time.perf_counter() - t0

    speedup = t_old / t_new
    print(f"  json   : {t_old:.3f}s  ({BENCHMARK_N / t_old:>12,.0f} calls/s)")
    print(f"  orjson : {t_new:.3f}s  ({BENCHMARK_N / t_new:>12,.0f} calls/s)")
    print(f"  속도 향상: {speedup:.1f}배")
    return {"t_old": t_old, "t_new": t_new, "speedup": speedup}


if __name__ == "__main__":
    identical = test_output_identical()
    result = test_benchmark()

    print()
    if not identical:
        print("❌ 출력 불일치 — 변경을 롤백하세요.")
        raise SystemExit(1)
    if result["speedup"] < 2.0:
        print(f"⚠️  speedup={result['speedup']:.1f}x — 예상보다 낮음, 환경을 확인하세요.")
    else:
        print(f"✅ 검증 통과 (speedup={result['speedup']:.1f}x)")
