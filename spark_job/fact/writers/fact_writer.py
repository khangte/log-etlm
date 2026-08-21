from __future__ import annotations

from pyspark.sql import DataFrame

from ..settings import get_fact_stream_settings
from ...clickhouse.writer_base import write_clickhouse_stream


def write_event_log_stream(df: DataFrame):
    """팩트 이벤트 스트림을 적재한다."""
    settings = get_fact_stream_settings()
    return write_clickhouse_stream(
        df,
        settings.table_name,
        settings.checkpoint_dir,
        query_name="event_log_stream",
        stream_name="event_log",
        skip_empty=settings.skip_empty_batch,
        trigger_processing_time=(
            f"{settings.trigger_interval_sec} seconds"
            if settings.trigger_interval_sec
            else None
        ),
        pre_coalesce_partitions=settings.pre_coalesce_partitions,
    )
