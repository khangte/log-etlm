# -----------------------------------------------------------------------------
# 파일명 : tests/spark_job/test_clickhouse_settings.py
# 목적   : ClickHouseSettings 설정 파싱 및 JDBC 옵션 생성 검증
# 실행   : uv run python tests/spark_job/test_clickhouse_settings.py
# -----------------------------------------------------------------------------

from __future__ import annotations

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "spark_job"))

from spark_job.clickhouse.settings import load_clickhouse_settings

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


def test_build_jdbc_options_unchanged() -> bool:
    """build_jdbc_options가 기존 동작을 유지하는지 검증한다."""
    print("\n=== build_jdbc_options (변경 없음) ===")

    env = {
        "SPARK_CLICKHOUSE_URL": "jdbc:clickhouse://clickhouse:8123/analytics",
        "SPARK_CLICKHOUSE_USER": "log_user",
        "SPARK_CLICKHOUSE_PASSWORD": "secret",
        "SPARK_CLICKHOUSE_JDBC_BATCHSIZE": "10000",
    }
    s = load_clickhouse_settings(env)
    opts = s.build_jdbc_options("analytics.event_log")

    results = [
        check("driver=ClickHouseDriver",   opts.get("driver"),   "com.clickhouse.jdbc.ClickHouseDriver"),
        check("url 포함",                  "url" in opts,        True),
        check("dbtable 전달",              opts.get("dbtable"),  "analytics.event_log"),
        check("isolationLevel=NONE",       opts.get("isolationLevel"), "NONE"),
        check("batchsize 포함",            opts.get("batchsize"), "10000"),
    ]
    return all(results)


if __name__ == "__main__":
    results = [
        test_build_jdbc_options_unchanged(),
    ]
    print()
    if all(results):
        print(f"{PASS} 전체 통과")
    else:
        print(f"{FAIL} 실패 케이스 있음")
        raise SystemExit(1)
