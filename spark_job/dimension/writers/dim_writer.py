from __future__ import annotations

from pyspark.sql import DataFrame

from ...clickhouse.writer_base import ClickHouseBatchWriterBase

DIM_DATE_TABLE = "analytics.dim_date"
DIM_TIME_TABLE = "analytics.dim_time"
DIM_SERVICE_TABLE = "analytics.dim_service"
DIM_STATUS_TABLE = "analytics.dim_status_code"
DIM_USER_TABLE = "analytics.dim_user"


class ClickHouseDimWriter(ClickHouseBatchWriterBase):
    """디멘전 테이블 적재를 담당한다."""

    def write_dim_date(self, df: DataFrame):
        """dim_date 테이블에 적재한다."""
        self.write_batch(df, DIM_DATE_TABLE, deduplicate_keys=["date"])

    def write_dim_time(self, df: DataFrame):
        """dim_time 테이블에 적재한다."""
        self.write_batch(df, DIM_TIME_TABLE, deduplicate_keys=["time_key"])

    def write_dim_service(self, df: DataFrame):
        """dim_service 테이블에 적재한다."""
        self.write_batch(df, DIM_SERVICE_TABLE, deduplicate_keys=["service"])

    def write_dim_status(self, df: DataFrame):
        """dim_status_code 테이블에 적재한다."""
        self.write_batch(df, DIM_STATUS_TABLE, deduplicate_keys=["status_code"])

    def write_dim_user(self, df: DataFrame):
        """dim_user 테이블에 적재한다."""
        self.write_batch(df, DIM_USER_TABLE, deduplicate_keys=["user_id"])
