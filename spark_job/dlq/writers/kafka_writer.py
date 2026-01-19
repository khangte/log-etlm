from __future__ import annotations

import time

from pyspark.sql import DataFrame, functions as F

from ...batch_log import append_batch_log
from ..schema import DLQ_VALUE_COLUMNS
from ..settings import DlqKafkaSettings, get_dlq_kafka_settings
from ..transforms.build_dlq_kafka import build_dlq_kafka_df


class KafkaDlqWriter:
    """DLQ Kafka 적재를 담당한다."""

    def __init__(self, settings: DlqKafkaSettings | None = None):
        """DLQ Kafka 설정을 주입한다."""
        self._settings = settings or get_dlq_kafka_settings()

    def write_dlq_kafka_stream(self, bad_df: DataFrame, *, topic: str):
        """DLQ Kafka 스트림을 적재한다."""
        dlq_topic = topic
        if not dlq_topic:
            raise ValueError("DLQ Kafka topic is required")
        bootstrap = self._settings.bootstrap
        if not bootstrap:
            raise ValueError("KAFKA_BOOTSTRAP is required for DLQ Kafka writer")
        trigger_processing_time = self._settings.trigger_interval or None
        log_empty = self._settings.log_empty

        query_name = "fact_event_dlq_kafka_stream"
        stream_name = "dlq_kafka"
        prefix = (
            "[spark batch] "
            f"stream={stream_name} table=dlq_kafka query={query_name}"
        )

        def _foreach(batch_df: DataFrame, batch_id: int) -> None:
            """DLQ Kafka 배치 적재와 타이밍 로그를 처리한다."""
            start_time = time.perf_counter()
            row_count = int(batch_df.count())
            row_line = f"{prefix} batch_id={batch_id} rows={row_count}"
            print(row_line)
            append_batch_log(row_line)
            if row_count == 0:
                if log_empty:
                    elapsed = time.perf_counter() - start_time
                    line = f"{prefix} batch_id={batch_id} empty=true duration={elapsed:.3f}s"
                    print(line)
                    append_batch_log(line)
                return

            payload_df = build_dlq_kafka_df(batch_df)
            value_struct = F.struct(*[F.col(name) for name in DLQ_VALUE_COLUMNS])
            kafka_df = payload_df.select(
                F.coalesce(F.col("source_key"), F.col("event_id")).cast("string").alias("key"),
                F.to_json(value_struct).alias("value"),
            )
            (
                kafka_df.write.format("kafka")
                .option("kafka.bootstrap.servers", bootstrap)
                .option("topic", dlq_topic)
                .save()
            )
            elapsed = time.perf_counter() - start_time
            line = f"{prefix} batch_id={batch_id} duration={elapsed:.3f}s"
            print(line)
            append_batch_log(line)

        writer = (
            bad_df.writeStream.foreachBatch(_foreach)
            .option("checkpointLocation", self._settings.checkpoint_dir)
        )
        if trigger_processing_time:
            writer = writer.trigger(processingTime=trigger_processing_time)
        writer = writer.queryName(query_name)
        return writer.start()
