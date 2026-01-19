from __future__ import annotations

from pyspark.sql import DataFrame

from ..settings import FactStreamSettings, get_fact_stream_settings
from ...clickhouse.writer_base import ClickHouseStreamWriterBase


class ClickHouseFactWriter(ClickHouseStreamWriterBase):
    """팩트 이벤트 적재를 담당한다."""

    def __init__(self, settings: FactStreamSettings | None = None):
        """팩트 이벤트 스트림 설정을 주입한다."""
        super().__init__()
        self._settings = settings or get_fact_stream_settings()

    def write_fact_event_stream(self, df: DataFrame):
        """팩트 이벤트 스트림을 적재한다."""
        trigger_processing_time = self._settings.trigger_interval or None
        return self.write_stream(
            df,
            self._settings.table_name,
            self._settings.checkpoint_dir,
            query_name="fact_event_stream",
            stream_name="fact_event",
            trigger_processing_time=trigger_processing_time,
        )
