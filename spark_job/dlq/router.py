from __future__ import annotations

import os

from .helpers import align_dlq_columns, get_dlq_mode, publish_dlq_to_kafka
from .writers.dlq_writer import FACT_EVENT_DLQ_TABLE
from ..clickhouse.sink import write_to_clickhouse


def handle_dlq_records(dlq_df, batch_id: int) -> None:
    """DLQ 저장 경로에 따라 direct 또는 kafka로 처리한다."""
    aligned_df = align_dlq_columns(dlq_df)
    if not aligned_df.take(1):
        return

    mode = get_dlq_mode()
    if mode == "direct":
        write_to_clickhouse(
            aligned_df,
            FACT_EVENT_DLQ_TABLE,
            batch_id=batch_id,
        )
        return

    dlq_topic = os.getenv("SPARK_DLQ_TOPIC", "logs.dlq")
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP")
    publish_dlq_to_kafka(aligned_df, dlq_topic, kafka_bootstrap)
