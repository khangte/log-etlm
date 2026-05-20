from __future__ import annotations

from dataclasses import dataclass
import os
import re
from typing import Mapping

from common.get_env import get_env_bool, get_env_int, get_env_str

_JDBC_HOST_PORT_RE = re.compile(r"jdbc:clickhouse://([^:/]+)(?::(\d+))?")


def _parse_jdbc_host_port(url: str) -> tuple[str, int]:
    """JDBC URL에서 host와 http_port를 추출한다."""
    m = _JDBC_HOST_PORT_RE.match(url)
    if not m:
        raise ValueError(f"JDBC URL에서 host:port를 파싱할 수 없습니다: {url!r}")
    return m.group(1), int(m.group(2) or "8123")


@dataclass(frozen=True)
class ClickHouseSettings:
    """ClickHouse 접속 및 쓰기 설정을 담는다."""
    url: str
    user: str | None
    password: str | None
    jdbc_batchsize: int | None
    jdbc_fetchsize: int | None
    write_partitions: int | None
    allow_repartition: bool
    retry_max: int
    fail_on_error: bool
    dlq_on_final_failure: bool
    dlq_topic: str | None
    kafka_bootstrap: str | None
    batch_guard_enabled: bool
    batch_guard_table: str

    def build_jdbc_options(self, table_name: str) -> dict[str, str]:
        """JDBC 옵션 딕셔너리를 생성한다."""
        if not self.url:
            raise ValueError("SPARK_CLICKHOUSE_URL is required")
        options: dict[str, str] = {
            "driver": "com.clickhouse.jdbc.ClickHouseDriver",
            "url": self.url,
            "dbtable": table_name,
            "isolationLevel": "NONE",
        }
        if self.user:
            options["user"] = self.user
        if self.password:
            options["password"] = self.password
        if self.jdbc_batchsize is not None:
            options["batchsize"] = str(self.jdbc_batchsize)
        if self.jdbc_fetchsize is not None:
            options["fetchsize"] = str(self.jdbc_fetchsize)
        return options

    def build_native_options(self, table_name: str) -> dict[str, str]:
        """spark-clickhouse-connector Native 옵션 딕셔너리를 생성한다."""
        if not self.url:
            raise ValueError("SPARK_CLICKHOUSE_URL is required")
        host, http_port = _parse_jdbc_host_port(self.url)
        options: dict[str, str] = {
            "host": host,
            "http_port": str(http_port),
            "protocol": "http",
            "table": table_name,
        }
        if self.user:
            options["user"] = self.user
        if self.password:
            options["password"] = self.password
        return options

    def build_catalog_configs(self, catalog_name: str = "clickhouse") -> dict[str, str]:
        """Spark SQL Catalog API용 설정 딕셔너리를 생성한다."""
        if not self.url:
            raise ValueError("SPARK_CLICKHOUSE_URL is required")
        host, http_port = _parse_jdbc_host_port(self.url)
        configs: dict[str, str] = {
            f"spark.sql.catalog.{catalog_name}": "com.clickhouse.spark.ClickHouseCatalog",
            f"spark.sql.catalog.{catalog_name}.host": host,
            f"spark.sql.catalog.{catalog_name}.http_port": str(http_port),
            f"spark.sql.catalog.{catalog_name}.protocol": "http",
        }
        if self.user:
            configs[f"spark.sql.catalog.{catalog_name}.user"] = self.user
        if self.password:
            configs[f"spark.sql.catalog.{catalog_name}.password"] = self.password
        return configs


@dataclass(frozen=True)
class BatchTimingLogSettings:
    """배치 타이밍 로그 설정을 담는다."""
    log_path: str | None


def load_clickhouse_settings(env: Mapping[str, str] | None = None) -> ClickHouseSettings:
    """환경 변수에서 ClickHouse 설정을 로드한다."""
    source = env or os.environ
    return ClickHouseSettings(
        url=get_env_str(source, "SPARK_CLICKHOUSE_URL", "") or "",
        user=get_env_str(source, "SPARK_CLICKHOUSE_USER"),
        password=get_env_str(source, "SPARK_CLICKHOUSE_PASSWORD"),
        jdbc_batchsize=get_env_int(source, "SPARK_CLICKHOUSE_JDBC_BATCHSIZE"),
        jdbc_fetchsize=get_env_int(source, "SPARK_CLICKHOUSE_JDBC_FETCHSIZE"),
        write_partitions=get_env_int(source, "SPARK_CLICKHOUSE_WRITE_PARTITIONS"),
        allow_repartition=get_env_bool(
            source,
            "SPARK_CLICKHOUSE_ALLOW_REPARTITION",
            False,
        ),
        retry_max=get_env_int(source, "SPARK_CLICKHOUSE_RETRY_MAX", 0) or 0,
        fail_on_error=get_env_bool(source, "SPARK_CLICKHOUSE_FAIL_ON_ERROR", True,),
        dlq_on_final_failure=get_env_bool(source, "SPARK_CLICKHOUSE_DLQ_ON_FINAL_FAILURE", True),
        dlq_topic=get_env_str(source, "SPARK_DLQ_TOPIC"),
        kafka_bootstrap=get_env_str(source, "KAFKA_BOOTSTRAP"),
        batch_guard_enabled=get_env_bool(
            source,
            "SPARK_CLICKHOUSE_BATCH_GUARD_ENABLED",
            True,
        ),
        batch_guard_table=(
            get_env_str(
                source,
                "SPARK_CLICKHOUSE_BATCH_GUARD_TABLE",
                "analytics.stream_batch_guard",
            )
            or "analytics.stream_batch_guard"
        ),
    )


def load_batch_timing_log_settings(
    env: Mapping[str, str] | None = None,
) -> BatchTimingLogSettings:
    """환경 변수에서 배치 타이밍 로그 설정을 로드한다."""
    source = env or os.environ
    return BatchTimingLogSettings(
        log_path=get_env_str(source, "SPARK_BATCH_TIMING_LOG_PATH"),
    )


_clickhouse_settings_cache: ClickHouseSettings | None = None
_batch_log_settings_cache: BatchTimingLogSettings | None = None


def get_clickhouse_settings() -> ClickHouseSettings:
    """캐시된 ClickHouse 설정을 반환한다."""
    global _clickhouse_settings_cache
    if _clickhouse_settings_cache is None:
        _clickhouse_settings_cache = load_clickhouse_settings()
    return _clickhouse_settings_cache


def get_batch_timing_log_settings() -> BatchTimingLogSettings:
    """캐시된 배치 타이밍 로그 설정을 반환한다."""
    global _batch_log_settings_cache
    if _batch_log_settings_cache is None:
        _batch_log_settings_cache = load_batch_timing_log_settings()
    return _batch_log_settings_cache
