from __future__ import annotations

from pyspark.sql import DataFrame

from ..sink import write_to_clickhouse

# ClickHouse 테이블 이름
FACT_EVENT_TABLE = "analytics.fact_event"

# 체크포인트 디렉터리
FACT_EVENT_CHECKPOINT_DIR = "/data/log-etlm/spark_checkpoints/fact_event"


class ClickHouseStreamWriter:
    def __init__(self, foreach_writer=write_to_clickhouse):
        self._foreach_writer = foreach_writer

    def _write_stream(
        self,
        df: DataFrame,
        table_name: str,
        checkpoint_dir: str,
        output_mode: str = "append",
        query_name: str | None = None,
        deduplicate_keys: list[str] | None = None,
    ):
        def _foreach(batch_df: DataFrame, _batch_id: int):
            out_df = batch_df
            if deduplicate_keys:
                # 배치 내 중복을 제거해 ClickHouse 중복 기록을 줄인다.
                out_df = out_df.dropDuplicates(deduplicate_keys) 
            self._foreach_writer(out_df, table_name, batch_id=_batch_id)

        writer = (
            df.writeStream
            .outputMode(output_mode)
            .foreachBatch(_foreach)
            .option("checkpointLocation", checkpoint_dir)
        )
        if query_name:
            writer = writer.queryName(query_name)
        return writer.start()

    def write_fact_event_stream(self, df: DataFrame):
        return self._write_stream(
            df,
            FACT_EVENT_TABLE,
            FACT_EVENT_CHECKPOINT_DIR,
            query_name="fact_event_stream",
        )
