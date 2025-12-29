from __future__ import annotations

from pyspark.sql import DataFrame

from ..sink import write_to_clickhouse

DIM_DATE_TABLE = "analytics.dim_date"
DIM_TIME_TABLE = "analytics.dim_time"
DIM_SERVICE_TABLE = "analytics.dim_service"
DIM_STATUS_TABLE = "analytics.dim_status_code"
DIM_USER_TABLE = "analytics.dim_user"


class ClickHouseDimWriter:
    def __init__(self, batch_writer=write_to_clickhouse):
        self._batch_writer = batch_writer

    def _write_batch(
        self,
        df: DataFrame,
        table_name: str,
        deduplicate_keys: list[str] | None = None,
    ):
        out_df = df
        if deduplicate_keys:
            out_df = out_df.dropDuplicates(deduplicate_keys)
        self._batch_writer(out_df, table_name)

    def write_dim_date(self, df: DataFrame):
        self._write_batch(df, DIM_DATE_TABLE, deduplicate_keys=["date"])

    def write_dim_time(self, df: DataFrame):
        self._write_batch(df, DIM_TIME_TABLE, deduplicate_keys=["time_key"])

    def write_dim_service(self, df: DataFrame):
        self._write_batch(df, DIM_SERVICE_TABLE, deduplicate_keys=["service"])

    def write_dim_status(self, df: DataFrame):
        self._write_batch(df, DIM_STATUS_TABLE, deduplicate_keys=["status_code"])

    def write_dim_user(self, df: DataFrame):
        self._write_batch(df, DIM_USER_TABLE, deduplicate_keys=["user_id"])
