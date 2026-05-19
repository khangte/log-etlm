# -----------------------------------------------------------------------------
# 파일명 : tests/spark_job/test_fact_settings.py
# 목적   : FactStreamSettings 환경변수 파싱 검증 (watermark / dedup_keys 중심)
# 실행   : uv run python tests/spark_job/test_fact_settings.py
# -----------------------------------------------------------------------------

from __future__ import annotations

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "spark_job"))

from spark_job.fact.settings import _parse_dedup_keys, load_fact_stream_settings

PASS = "✅"
FAIL = "❌"


def check(label: str, actual, expected) -> bool:
    """단일 케이스를 검증하고 결과를 출력한다."""
    ok = actual == expected
    status = PASS if ok else FAIL
    print(f"  {status} {label}")
    if not ok:
        print(f"       expected: {expected!r}")
        print(f"       actual  : {actual!r}")
    return ok


def test_parse_dedup_keys() -> bool:
    """_parse_dedup_keys 파싱 케이스를 검증한다."""
    print("=== _parse_dedup_keys ===")
    results = [
        check("None 입력 → None",         _parse_dedup_keys(None),            None),
        check("빈 문자열 → None",          _parse_dedup_keys(""),              None),
        check("쉼표만 → None",             _parse_dedup_keys(","),             None),
        check("단일 키",                   _parse_dedup_keys("event_id"),      ["event_id"]),
        check("복수 키",                   _parse_dedup_keys("event_id,user_id"), ["event_id", "user_id"]),
        check("공백 포함 복수 키",          _parse_dedup_keys("event_id, user_id"), ["event_id", "user_id"]),
    ]
    return all(results)


def test_watermark_settings() -> bool:
    """SPARK_FACT_DEDUP_WATERMARK 환경변수 파싱을 검증한다."""
    print("\n=== SPARK_FACT_DEDUP_WATERMARK ===")

    def settings(env: dict) -> str | None:
        return load_fact_stream_settings(env).deduplicate_watermark

    results = [
        check("10 minutes 설정",  settings({"SPARK_FACT_DEDUP_WATERMARK": "10 minutes"}), "10 minutes"),
        check("1 hour 설정",      settings({"SPARK_FACT_DEDUP_WATERMARK": "1 hour"}),     "1 hour"),
        check("미설정 → None",    settings({}),                                            None),
        check("빈 문자열 → None", settings({"SPARK_FACT_DEDUP_WATERMARK": ""}),           None),
        check("공백만 → None",    settings({"SPARK_FACT_DEDUP_WATERMARK": "  "}),         None),
    ]
    return all(results)


def test_dedup_keys_and_watermark_together() -> bool:
    """SPARK_FACT_DEDUP_KEYS + SPARK_FACT_DEDUP_WATERMARK 조합을 검증한다."""
    print("\n=== dedup_keys + watermark 조합 ===")
    env = {
        "SPARK_FACT_DEDUP_KEYS": "event_id",
        "SPARK_FACT_DEDUP_WATERMARK": "10 minutes",
    }
    s = load_fact_stream_settings(env)
    results = [
        check("keys 파싱",      s.deduplicate_keys,      ["event_id"]),
        check("watermark 파싱", s.deduplicate_watermark, "10 minutes"),
    ]

    # keys만 있고 watermark 없는 경우 — foreachBatch dedup 경로
    env_keys_only = {"SPARK_FACT_DEDUP_KEYS": "event_id"}
    s2 = load_fact_stream_settings(env_keys_only)
    results += [
        check("keys만 설정 시 watermark=None", s2.deduplicate_watermark, None),
        check("keys만 설정 시 keys 존재",      s2.deduplicate_keys,      ["event_id"]),
    ]
    return all(results)


if __name__ == "__main__":
    results = [
        test_parse_dedup_keys(),
        test_watermark_settings(),
        test_dedup_keys_and_watermark_together(),
    ]
    print()
    if all(results):
        print(f"{PASS} 전체 통과")
    else:
        print(f"{FAIL} 실패 케이스 있음")
        raise SystemExit(1)
