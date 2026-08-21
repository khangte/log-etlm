from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import os
from typing import Mapping

from common.get_env import get_env_int, get_env_str


@dataclass(frozen=True)
class ClickHouseSettings:
    """ClickHouse 접속 및 쓰기 설정을 담는다."""
    url: str
    user: str | None
    password: str | None
    jdbc_batchsize: int | None
    write_partitions: int | None

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
        return options


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
        write_partitions=get_env_int(source, "SPARK_CLICKHOUSE_WRITE_PARTITIONS"),
    )


def load_batch_timing_log_settings(
    env: Mapping[str, str] | None = None,
) -> BatchTimingLogSettings:
    """환경 변수에서 배치 타이밍 로그 설정을 로드한다."""
    source = env or os.environ
    return BatchTimingLogSettings(
        log_path=get_env_str(source, "SPARK_BATCH_TIMING_LOG_PATH"),
    )


@lru_cache(maxsize=1)
def get_clickhouse_settings() -> ClickHouseSettings:
    """캐시된 ClickHouse 설정을 반환한다."""
    return load_clickhouse_settings()


@lru_cache(maxsize=1)
def get_batch_timing_log_settings() -> BatchTimingLogSettings:
    """캐시된 배치 타이밍 로그 설정을 반환한다."""
    return load_batch_timing_log_settings()
