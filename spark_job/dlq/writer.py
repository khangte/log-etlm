from __future__ import annotations

from pyspark.sql import DataFrame

from ..clickhouse.writer_base import ClickHouseStreamWriterBase

# ClickHouse 테이블 이름
FACT_EVENT_DLQ_TABLE = "analytics.fact_event_dlq"

# 체크포인트 디렉터리
FACT_EVENT_DLQ_CHECKPOINT_DIR = "/data/log-etlm/spark_checkpoints/fact_event_dlq"


class ClickHouseDlqWriter(ClickHouseStreamWriterBase):
    def write_dlq_stream(self, df: DataFrame):
        """DLQ 스트림을 ClickHouse로 적재한다."""
        return self.write_stream(
            df,
            FACT_EVENT_DLQ_TABLE,
            FACT_EVENT_DLQ_CHECKPOINT_DIR,
            query_name="fact_event_dlq_stream",
        )
