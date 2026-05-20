# -----------------------------------------------------------------------------
# 파일명 : tests/spark_job/test_clickhouse_settings.py
# 목적   : ClickHouseSettings 설정 파싱 및 native connector 옵션 생성 검증
# 실행   : uv run python tests/spark_job/test_clickhouse_settings.py
# -----------------------------------------------------------------------------

from __future__ import annotations

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "spark_job"))

from spark_job.clickhouse.settings import _parse_jdbc_host_port, load_clickhouse_settings

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


def test_parse_jdbc_host_port() -> bool:
    """_parse_jdbc_host_port URL 파싱 케이스를 검증한다."""
    print("=== _parse_jdbc_host_port ===")
    results = [
        check(
            "host:port 명시",
            _parse_jdbc_host_port("jdbc:clickhouse://clickhouse:8123/analytics"),
            ("clickhouse", 8123),
        ),
        check(
            "쿼리 파라미터 포함",
            _parse_jdbc_host_port(
                "jdbc:clickhouse://clickhouse:8123/analytics?compress=0&socket_timeout=600000"
            ),
            ("clickhouse", 8123),
        ),
        check(
            "포트 생략 → 기본값 8123",
            _parse_jdbc_host_port("jdbc:clickhouse://clickhouse/analytics"),
            ("clickhouse", 8123),
        ),
        check(
            "IP 주소 호스트",
            _parse_jdbc_host_port("jdbc:clickhouse://192.168.1.10:9000/db"),
            ("192.168.1.10", 9000),
        ),
    ]

    # 잘못된 URL → ValueError
    try:
        _parse_jdbc_host_port("http://clickhouse:8123")
        results.append(check("잘못된 URL → ValueError", "예외 미발생", "ValueError"))
    except ValueError:
        results.append(check("잘못된 URL → ValueError", True, True))

    return all(results)


def test_build_native_options() -> bool:
    """build_native_options 옵션 딕셔너리 생성을 검증한다."""
    print("\n=== build_native_options ===")

    base_env = {
        "SPARK_CLICKHOUSE_URL": "jdbc:clickhouse://clickhouse:8123/analytics?compress=0",
        "SPARK_CLICKHOUSE_USER": "log_user",
        "SPARK_CLICKHOUSE_PASSWORD": "secret",
    }
    s = load_clickhouse_settings(base_env)
    opts = s.build_native_options("analytics.fact_event")

    results = [
        check("host 추출",             opts.get("host"),       "clickhouse"),
        check("http_port 추출",        opts.get("http_port"),  "8123"),
        check("protocol=http",         opts.get("protocol"),   "http"),
        check("table 전달",            opts.get("table"),      "analytics.fact_event"),
        check("user 포함",             opts.get("user"),       "log_user"),
        check("password 포함",         opts.get("password"),   "secret"),
        check("jdbc driver 미포함",    "driver" not in opts,   True),
        check("dbtable 미포함",        "dbtable" not in opts,  True),
    ]

    # user/password 없는 경우 → 옵션에서 제외
    env_no_auth = {"SPARK_CLICKHOUSE_URL": "jdbc:clickhouse://clickhouse:8123/analytics"}
    s2 = load_clickhouse_settings(env_no_auth)
    opts2 = s2.build_native_options("analytics.fact_event")
    results += [
        check("user 없으면 옵션 제외",     "user" not in opts2,     True),
        check("password 없으면 옵션 제외", "password" not in opts2, True),
    ]

    # 테이블명 변경 시 table 옵션 반영
    opts3 = s.build_native_options("analytics.stream_batch_guard")
    results.append(check("table 옵션 변경 반영", opts3.get("table"), "analytics.stream_batch_guard"))

    return all(results)


def test_build_jdbc_options_unchanged() -> bool:
    """build_jdbc_options가 기존 동작을 유지하는지 검증한다 (배치 가드용)."""
    print("\n=== build_jdbc_options (배치 가드용 JDBC, 변경 없음) ===")

    env = {
        "SPARK_CLICKHOUSE_URL": "jdbc:clickhouse://clickhouse:8123/analytics",
        "SPARK_CLICKHOUSE_USER": "log_user",
        "SPARK_CLICKHOUSE_PASSWORD": "secret",
        "SPARK_CLICKHOUSE_JDBC_BATCHSIZE": "10000",
    }
    s = load_clickhouse_settings(env)
    opts = s.build_jdbc_options("analytics.stream_batch_guard")

    results = [
        check("driver=ClickHouseDriver",   opts.get("driver"),   "com.clickhouse.jdbc.ClickHouseDriver"),
        check("url 포함",                  "url" in opts,        True),
        check("dbtable 전달",              opts.get("dbtable"),  "analytics.stream_batch_guard"),
        check("isolationLevel=NONE",       opts.get("isolationLevel"), "NONE"),
        check("batchsize 포함",            opts.get("batchsize"), "10000"),
    ]
    return all(results)

def test_build_catalog_configs() -> bool:
    """build_catalog_configs Catalog API 설정 생성을 검증한다."""
    print("\n=== build_catalog_configs ===")

    env = {
        "SPARK_CLICKHOUSE_URL": "jdbc:clickhouse://clickhouse:8123/analytics",
        "SPARK_CLICKHOUSE_USER": "log_user",
        "SPARK_CLICKHOUSE_PASSWORD": "secret",
    }
    s = load_clickhouse_settings(env)
    cfg = s.build_catalog_configs()

    results = [
        check("catalog class 설정",   cfg.get("spark.sql.catalog.clickhouse"), "com.clickhouse.spark.ClickHouseCatalog"),
        check("host 추출",             cfg.get("spark.sql.catalog.clickhouse.host"), "clickhouse"),
        check("http_port 추출",        cfg.get("spark.sql.catalog.clickhouse.http_port"), "8123"),
        check("protocol=http",         cfg.get("spark.sql.catalog.clickhouse.protocol"), "http"),
        check("user 포함",             cfg.get("spark.sql.catalog.clickhouse.user"), "log_user"),
        check("password 포함",         cfg.get("spark.sql.catalog.clickhouse.password"), "secret"),
    ]

    # catalog_name 파라미터 적용 확인
    cfg2 = s.build_catalog_configs(catalog_name="ch")
    results += [
        check("catalog_name 변경 반영", cfg2.get("spark.sql.catalog.ch"), "com.clickhouse.spark.ClickHouseCatalog"),
        check("기존 catalog_name 미포함", "spark.sql.catalog.clickhouse" not in cfg2, True),
    ]

    return all(results)


if __name__ == "__main__":
    results = [
        test_parse_jdbc_host_port(),
        test_build_native_options(),
        test_build_jdbc_options_unchanged(),
        test_build_catalog_configs(),
    ]
    print()
    if all(results):
        print(f"{PASS} 전체 통과")
    else:
        print(f"{FAIL} 실패 케이스 있음")
        raise SystemExit(1)
