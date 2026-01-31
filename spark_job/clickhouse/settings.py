from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Dict, Mapping, Optional

from common.get_env import get_env_bool, get_env_float, get_env_int, get_env_str


@dataclass(frozen=True)
class ClickHouseSettings:
    """ClickHouse 접속 및 쓰기 설정을 담는다."""
    url: str
    user: Optional[str]
    password: Optional[str]
    jdbc_batchsize: Optional[int]
    jdbc_fetchsize: Optional[int]
    write_partitions: Optional[int]
    allow_repartition: bool
    retry_max: int
    retry_backoff_sec: float
    fail_on_error: bool

    def build_jdbc_options(self, table_name: str) -> Dict[str, str]:
        """JDBC 옵션 딕셔너리를 생성한다."""
        if not self.url:
            raise ValueError("SPARK_CLICKHOUSE_URL is required")
        options: Dict[str, str] = {
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


@dataclass(frozen=True)
class BatchTimingLogSettings:
    """배치 타이밍 로그 설정을 담는다."""
    log_path: Optional[str]


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
        retry_backoff_sec=get_env_float(source, "SPARK_CLICKHOUSE_RETRY_BACKOFF_SEC", 0.0) or 0.0,
        fail_on_error=get_env_bool(source, "SPARK_CLICKHOUSE_FAIL_ON_ERROR", True,),
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
