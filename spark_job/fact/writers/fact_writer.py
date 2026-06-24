from __future__ import annotations

from pyspark.sql import DataFrame

from ..settings import FactStreamSettings, get_fact_stream_settings
from ...clickhouse.writer_base import write_clickhouse_stream


class ClickHouseFactWriter:
    """팩트 이벤트 적재를 담당한다."""

    def __init__(self, settings: FactStreamSettings | None = None):
        self._settings = settings or get_fact_stream_settings()

    def write_event_log_stream(self, df: DataFrame):
        """팩트 이벤트 스트림을 적재한다."""
        settings = self._settings
        out_df = df
        batch_dedup_keys = settings.deduplicate_keys
        dedup_watermark = settings.deduplicate_watermark

        if batch_dedup_keys and dedup_watermark:
            if "event_timestamp" not in out_df.columns:
                raise ValueError(
                    "event_timestamp column is required for streaming dedup watermark"
                )
            missing_keys = [key for key in batch_dedup_keys if key not in out_df.columns]
            if missing_keys:
                raise ValueError(
                    "missing dedup columns for fact stream: "
                    + ",".join(missing_keys)
                )
            out_df = (
                out_df.withWatermark("event_timestamp", dedup_watermark)
                .dropDuplicatesWithinWatermark(batch_dedup_keys)
            )
            # 스트리밍 상태 기반 dedup을 적용한 경우 foreachBatch dedup은 생략한다.
            batch_dedup_keys = None

        return write_clickhouse_stream(
            out_df,
            settings.table_name,
            settings.checkpoint_dir,
            query_name="event_log_stream",
            stream_name="event_log",
            deduplicate_keys=batch_dedup_keys,
            skip_empty=settings.skip_empty_batch,
            trigger_processing_time=settings.trigger_interval,
            pre_coalesce_partitions=settings.pre_coalesce_partitions,
        )
