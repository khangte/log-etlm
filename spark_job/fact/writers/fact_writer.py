from __future__ import annotations

import os

from pyspark.sql import DataFrame

from ...clickhouse.writer_base import ClickHouseStreamWriterBase

# ClickHouse 테이블 이름
FACT_EVENT_TABLE = "analytics.fact_event"

# 체크포인트 디렉터리
FACT_EVENT_CHECKPOINT_DIR = "/data/log-etlm/spark_checkpoints/fact_event"


class ClickHouseFactWriter(ClickHouseStreamWriterBase):
    def write_fact_event_stream(self, df: DataFrame):
        """write_fact_event_stream 처리를 수행한다."""
        trigger_processing_time = os.getenv("SPARK_FACT_TRIGGER_INTERVAL").strip() or None
        return self.write_stream(
            df,
            FACT_EVENT_TABLE,
            FACT_EVENT_CHECKPOINT_DIR,
            query_name="fact_event_stream",
            trigger_processing_time=trigger_processing_time,
        )
