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
        out_df = df
        batch_dedup_keys = self._settings.deduplicate_keys
        dedup_watermark = self._settings.deduplicate_watermark

        if batch_dedup_keys and dedup_watermark:
            if "event_ts" not in out_df.columns:
                raise ValueError(
                    "event_ts column is required for streaming dedup watermark"
                )
            missing_keys = [key for key in batch_dedup_keys if key not in out_df.columns]
            if missing_keys:
                raise ValueError(
                    "missing dedup columns for fact stream: "
                    + ",".join(missing_keys)
                )
            out_df = (
                out_df.withWatermark("event_ts", dedup_watermark)
                .dropDuplicatesWithinWatermark(batch_dedup_keys)
            )
            # 스트리밍 상태 기반 dedup을 적용한 경우 foreachBatch dedup은 생략한다.
            batch_dedup_keys = None

        return self.write_stream(
            out_df,
            self._settings.table_name,
            self._settings.checkpoint_dir,
            query_name="fact_event_stream",
            stream_name="fact_event",
            deduplicate_keys=batch_dedup_keys,
            skip_empty=self._settings.skip_empty_batch,
            trigger_processing_time=trigger_processing_time,
            pre_coalesce_partitions=self._settings.num_partitions,
        )
