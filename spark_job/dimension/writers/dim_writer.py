from __future__ import annotations

from pyspark.sql import DataFrame

from ...clickhouse.writer_base import ClickHouseBatchWriterBase

DIM_DATE_TABLE = "analytics.dim_date"
DIM_TIME_TABLE = "analytics.dim_time"
DIM_SERVICE_TABLE = "analytics.dim_service"
DIM_STATUS_TABLE = "analytics.dim_status_code"
DIM_USER_TABLE = "analytics.dim_user"


class ClickHouseDimWriter(ClickHouseBatchWriterBase):
    def write_dim_date(self, df: DataFrame):
        """write_dim_date 처리를 수행한다."""
        self.write_batch(df, DIM_DATE_TABLE, deduplicate_keys=["date"])

    def write_dim_time(self, df: DataFrame):
        """write_dim_time 처리를 수행한다."""
        self.write_batch(df, DIM_TIME_TABLE, deduplicate_keys=["time_key"])

    def write_dim_service(self, df: DataFrame):
        """write_dim_service 처리를 수행한다."""
        self.write_batch(df, DIM_SERVICE_TABLE, deduplicate_keys=["service"])

    def write_dim_status(self, df: DataFrame):
        """write_dim_status 처리를 수행한다."""
        self.write_batch(df, DIM_STATUS_TABLE, deduplicate_keys=["status_code"])

    def write_dim_user(self, df: DataFrame):
        """write_dim_user 처리를 수행한다."""
        self.write_batch(df, DIM_USER_TABLE, deduplicate_keys=["user_id"])
