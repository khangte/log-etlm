from __future__ import annotations

import os
import time

from pyspark.sql import DataFrame, functions as F

from ..schema import DLQ_VALUE_COLUMNS
from ..transforms.build_dlq_kafka import build_dlq_kafka_df

# 체크포인트 디렉터리
FACT_EVENT_DLQ_KAFKA_CHECKPOINT_DIR = "/data/log-etlm/spark_checkpoints/fact_event_dlq_kafka"


class KafkaDlqWriter:
    def write_dlq_kafka_stream(self, bad_df: DataFrame, *, topic: str):
        """파싱/검증 실패 레코드를 DLQ Kafka 토픽으로 적재한다."""
        dlq_topic = topic
        bootstrap = os.getenv("KAFKA_BOOTSTRAP")
        if not bootstrap:
            raise ValueError("KAFKA_BOOTSTRAP is required for DLQ Kafka writer")
        trigger_processing_time = os.getenv("SPARK_DLQ_KAFKA_TRIGGER_INTERVAL").strip() or None
        log_empty = os.getenv("SPARK_DLQ_KAFKA_LOG_EMPTY", "false").strip().lower() in (
            "1",
            "true",
            "yes",
            "y",
        )

        def _foreach(batch_df: DataFrame, batch_id: int) -> None:
            """DLQ Kafka 배치 적재와 타이밍 로그를 처리한다."""
            start_time = time.perf_counter()
            if batch_df.rdd.isEmpty():
                if log_empty:
                    elapsed = time.perf_counter() - start_time
                    print(
                        "[spark batch] "
                        f"stream=dlq_kafka batch_id={batch_id} empty=true duration={elapsed:.3f}s"
                    )
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
            print(
                "[spark batch] "
                f"stream=dlq_kafka batch_id={batch_id} duration={elapsed:.3f}s"
            )

        writer = (
            bad_df.writeStream.foreachBatch(_foreach)
            .option("checkpointLocation", FACT_EVENT_DLQ_KAFKA_CHECKPOINT_DIR)
        )
        if trigger_processing_time:
            writer = writer.trigger(processingTime=trigger_processing_time)
        query_name = "fact_event_dlq_kafka_stream"
        writer = writer.queryName(query_name)
        return writer.start()
