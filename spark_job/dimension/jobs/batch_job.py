# 파일명 : spark_job/dimension/jobs/batch_job.py
# 목적   : 하루 1회 배치로 디멘전 테이블을 갱신한다.
# 참고   : 자동 실행되지 않으며, 수동 또는 크론으로 실행해야 한다.

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from pyspark.sql import DataFrame, SparkSession

from ...clickhouse.settings import ClickHouseSettings, get_clickhouse_settings
from ...spark import build_batch_spark
from ..parsers import (
    parse_dim_date,
    parse_dim_service,
    parse_dim_status_code,
    parse_dim_time,
    parse_dim_user,
)
from ..settings import DimensionBatchSettings, get_dimension_batch_settings
from ..writers.dim_writer import ClickHouseDimWriter

_FACT_TABLE = "analytics.fact_event"


@dataclass(frozen=True)
class DimensionBatchJob:
    """디멘전 배치 작업을 실행한다."""
    settings: DimensionBatchSettings
    clickhouse_settings: ClickHouseSettings
    writer: ClickHouseDimWriter

    def run(self) -> None:
        """디멘전 배치 작업을 수행한다."""
        spark = build_batch_spark()
        spark.sparkContext.setLogLevel("INFO")
        try:
            fact_df = self._read_fact_event(spark)
            service_map_df = self._read_service_map(spark)

            dim_date_df = parse_dim_date(fact_df, time_col="ingest_ts")
            dim_time_df = parse_dim_time(fact_df, time_col="ingest_ts")
            dim_service_df = parse_dim_service(fact_df, service_map_df=service_map_df)
            dim_status_df = parse_dim_status_code(fact_df)
            dim_user_df = parse_dim_user(fact_df)

            self.writer.write_dim_date(dim_date_df)
            self.writer.write_dim_time(dim_time_df)
            self.writer.write_dim_service(dim_service_df)
            self.writer.write_dim_status(dim_status_df)
            self.writer.write_dim_user(dim_user_df)
        finally:
            spark.stop()

    def _read_fact_event(self, spark: SparkSession) -> DataFrame:
        """팩트 이벤트를 읽는다."""
        # 최근 N일 데이터만 읽어서 디멘전을 갱신한다.
        query = f"""(
            SELECT ingest_ts, event_ts, service, status_code, user_id
            FROM {_FACT_TABLE}
            WHERE ingest_ts >= now() - INTERVAL {self.settings.lookback_days} DAY
        ) AS fact"""

        reader = spark.read.format("jdbc")
        for key, value in self.clickhouse_settings.build_jdbc_options(query).items():
            reader = reader.option(key, value)
        return reader.load()

    def _read_service_map(self, spark: SparkSession) -> Optional[DataFrame]:
        """서비스 메타 매핑 파일을 읽는다."""
        path = (self.settings.service_map_path or "").strip()
        if not path:
            return None
        return (
            spark.read
            .option("header", "true")
            .option("inferSchema", "false")
            .csv(path)
            .select("service", "service_group", "is_active", "description")
        )


def run_dim_batch() -> None:
    """디멘전 배치 작업을 시작한다."""
    settings = get_dimension_batch_settings()
    clickhouse_settings = get_clickhouse_settings()
    job = DimensionBatchJob(
        settings=settings,
        clickhouse_settings=clickhouse_settings,
        writer=ClickHouseDimWriter(),
    )
    job.run()


if __name__ == "__main__":
    run_dim_batch()
