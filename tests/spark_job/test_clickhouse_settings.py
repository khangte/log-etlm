# -----------------------------------------------------------------------------
# 파일명 : tests/spark_job/test_clickhouse_settings.py
# 목적   : ClickHouseSettings 설정 파싱 및 JDBC 옵션 생성 검증
# 실행   : uv run pytest tests/spark_job/test_clickhouse_settings.py -v
# -----------------------------------------------------------------------------

from __future__ import annotations

import pytest

from spark_job.clickhouse.settings import load_clickhouse_settings


@pytest.fixture(scope="module")
def opts():
    env = {
        "SPARK_CLICKHOUSE_URL": "jdbc:clickhouse://clickhouse:8123/analytics",
        "SPARK_CLICKHOUSE_USER": "log_user",
        "SPARK_CLICKHOUSE_PASSWORD": "secret",
        "SPARK_CLICKHOUSE_JDBC_BATCHSIZE": "10000",
    }
    s = load_clickhouse_settings(env)
    return s.build_jdbc_options("analytics.event_log")


@pytest.mark.parametrize(
    "key,expected",
    [
        ("driver", "com.clickhouse.jdbc.ClickHouseDriver"),
        ("dbtable", "analytics.event_log"),
        ("isolationLevel", "NONE"),
        ("batchsize", "10000"),
    ],
)
def test_build_jdbc_options_unchanged(opts, key, expected):
    assert opts.get(key) == expected


def test_build_jdbc_options_url_포함(opts):
    assert "url" in opts
