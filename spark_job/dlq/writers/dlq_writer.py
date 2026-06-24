from __future__ import annotations

from pyspark.sql import DataFrame

from ..settings import DlqStreamSettings, get_dlq_stream_settings
from ...clickhouse.writer_base import write_clickhouse_stream


class ClickHouseDlqWriter:
    """DLQ 스트림 적재를 담당한다."""

    def __init__(self, settings: DlqStreamSettings | None = None):
        self._settings = settings or get_dlq_stream_settings()

    def write_dlq_stream(self, df: DataFrame):
        """DLQ 스트림을 적재한다."""
        settings = self._settings
        return write_clickhouse_stream(
            df,
            settings.table_name,
            settings.checkpoint_dir,
            query_name="event_log_dlq_stream",
            stream_name="dlq_table",
            skip_empty=True,
            trigger_processing_time=settings.trigger_interval,
        )
