from __future__ import annotations

import os

from ..transforms.parse_dlq import build_dlq_clickhouse_df
from ...dlq.helpers import get_dlq_mode
from ...common.checkpoints import maybe_reset_checkpoint
from ..readers.dlq_kafka import read_dlq_stream
from ..writers.dlq_writer import ClickHouseDlqWriter, FACT_EVENT_DLQ_CHECKPOINT_DIR
from ...spark import build_streaming_spark


def start_fact_event_dlq_stream(spark=None):
    """DLQ 토픽을 읽어 fact_event_dlq로 적재한다."""
    dlq_enabled = os.getenv("SPARK_DLQ_ENABLED", "true").strip().lower() in (
        "1",
        "true",
        "yes",
        "y",
    )
    if not dlq_enabled:
        return None
    if get_dlq_mode() != "kafka":
        return None

    if spark is None:
        spark = build_streaming_spark(master=os.getenv("SPARK_MASTER_URL"))

    maybe_reset_checkpoint(FACT_EVENT_DLQ_CHECKPOINT_DIR)

    dlq_kafka_df = read_dlq_stream(spark)

    dlq_df = build_dlq_clickhouse_df(dlq_kafka_df)

    writer = ClickHouseDlqWriter()
    return writer.write_dlq_stream(dlq_df)
