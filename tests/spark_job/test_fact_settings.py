# -----------------------------------------------------------------------------
# 파일명 : tests/spark_job/test_fact_settings.py
# 목적   : FactStreamSettings 환경변수 파싱 검증 (watermark / dedup_keys 중심)
# 실행   : uv run pytest tests/spark_job/test_fact_settings.py -v
# -----------------------------------------------------------------------------

from __future__ import annotations

import pytest

from spark_job.fact.settings import _parse_dedup_keys, load_fact_stream_settings


@pytest.mark.parametrize(
    "raw,expected",
    [
        (None, None),
        ("", None),
        (",", None),
        ("event_id", ["event_id"]),
        ("event_id,user_id", ["event_id", "user_id"]),
        ("event_id, user_id", ["event_id", "user_id"]),
    ],
)
def test_parse_dedup_keys(raw, expected):
    assert _parse_dedup_keys(raw) == expected


@pytest.mark.parametrize(
    "env,expected",
    [
        ({"SPARK_FACT_DEDUP_WATERMARK": "10 minutes"}, "10 minutes"),
        ({"SPARK_FACT_DEDUP_WATERMARK": "1 hour"}, "1 hour"),
        ({}, None),
        ({"SPARK_FACT_DEDUP_WATERMARK": ""}, None),
        ({"SPARK_FACT_DEDUP_WATERMARK": "  "}, None),
    ],
)
def test_watermark_settings(env, expected):
    assert load_fact_stream_settings(env).deduplicate_watermark == expected


def test_dedup_keys_and_watermark_together():
    env = {
        "SPARK_FACT_DEDUP_KEYS": "event_id",
        "SPARK_FACT_DEDUP_WATERMARK": "10 minutes",
    }
    s = load_fact_stream_settings(env)
    assert s.deduplicate_keys == ["event_id"]
    assert s.deduplicate_watermark == "10 minutes"


def test_dedup_keys_only_no_watermark():
    env_keys_only = {"SPARK_FACT_DEDUP_KEYS": "event_id"}
    s2 = load_fact_stream_settings(env_keys_only)
    assert s2.deduplicate_watermark is None
    assert s2.deduplicate_keys == ["event_id"]
