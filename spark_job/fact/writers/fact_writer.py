from __future__ import annotations

from ...clickhouse.sink import write_to_clickhouse

FACT_EVENT_TABLE = "analytics.fact_event"
FACT_EVENT_CHECKPOINT_DIR = "/data/log-etlm/spark_checkpoints/fact_event"


def write_fact_event_batch(df, batch_id: int) -> None:
    """fact_event 마이크로배치를 ClickHouse로 적재한다."""
    write_to_clickhouse(df, FACT_EVENT_TABLE, batch_id=batch_id)
