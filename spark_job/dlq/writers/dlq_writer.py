from __future__ import annotations

from pyspark.sql import DataFrame

from ..settings import DlqStreamSettings, get_dlq_stream_settings
from ...clickhouse.writer_base import ClickHouseStreamWriterBase


class ClickHouseDlqWriter(ClickHouseStreamWriterBase):
    """DLQ 스트림 적재를 담당한다."""

    def __init__(self, settings: DlqStreamSettings | None = None):
        """DLQ 스트림 설정을 주입한다."""
        super().__init__()
        self._settings = settings or get_dlq_stream_settings()

    def write_dlq_stream(self, df: DataFrame):
        """DLQ 스트림을 적재한다."""
        trigger_processing_time = self._settings.trigger_interval or None
        return self.write_stream(
            df,
            self._settings.table_name,
            self._settings.checkpoint_dir,
            query_name="fact_event_dlq_stream",
            stream_name="dlq_table",
            skip_empty=True,
            trigger_processing_time=trigger_processing_time,
        )
