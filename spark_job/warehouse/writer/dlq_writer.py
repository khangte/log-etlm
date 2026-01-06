from __future__ import annotations

from pyspark.sql import DataFrame

from ..sink import write_to_clickhouse

# ClickHouse 테이블 이름
FACT_EVENT_DLQ_TABLE = "analytics.fact_event_dlq"

# 체크포인트 디렉터리
FACT_EVENT_DLQ_CHECKPOINT_DIR = "/data/log-etlm/spark_checkpoints/fact_event_dlq"


class ClickHouseDlqWriter:
    def __init__(self, foreach_writer=write_to_clickhouse):
        self._foreach_writer = foreach_writer

    def _write_stream(
        self,
        df: DataFrame,
        table_name: str,
        checkpoint_dir: str,
        output_mode: str = "append",
    ):
        def _foreach(batch_df: DataFrame, _batch_id: int):
            self._foreach_writer(batch_df, table_name, batch_id=_batch_id)

        return (
            df.writeStream
            .outputMode(output_mode)
            .foreachBatch(_foreach)
            .option("checkpointLocation", checkpoint_dir)
            .start()
        )

    def write_dlq_stream(self, df: DataFrame):
        return self._write_stream(df, FACT_EVENT_DLQ_TABLE, FACT_EVENT_DLQ_CHECKPOINT_DIR)
