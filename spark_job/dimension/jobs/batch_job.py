# spark_job/dimension/jobs/batch_job.py
# 하루 1회 배치로 Dimension 테이블을 갱신한다.
# NOTE: 이 파일은 자동 실행되지 않으며, 수동 또는 크론으로 실행해야 한다.

from __future__ import annotations

from ..parsers import (
    parse_dim_date,
    parse_dim_service,
    parse_dim_status_code,
    parse_dim_time,
    parse_dim_user,
)
from ..readers.fact_reader import read_fact_event
from ..writers.dim_writer import ClickHouseDimWriter
from ...spark import build_batch_spark


def run_dim_batch() -> None:
    """Dimension 테이블을 배치로 갱신한다."""
    spark = build_batch_spark()
    spark.sparkContext.setLogLevel("INFO")

    fact_df = read_fact_event(spark)

    dim_date_df = parse_dim_date(fact_df)
    dim_time_df = parse_dim_time(fact_df)
    dim_service_df = parse_dim_service(fact_df)
    dim_status_df = parse_dim_status_code(fact_df)
    dim_user_df = parse_dim_user(fact_df)

    writer = ClickHouseDimWriter()
    writer.write_dim_date(dim_date_df)
    writer.write_dim_time(dim_time_df)
    writer.write_dim_service(dim_service_df)
    writer.write_dim_status(dim_status_df)
    writer.write_dim_user(dim_user_df)

    spark.stop()


if __name__ == "__main__":
    run_dim_batch()
