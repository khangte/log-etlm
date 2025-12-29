from __future__ import annotations

from pyspark.sql import DataFrame

from ..sink import write_to_clickhouse

# ClickHouse 테이블 이름
FACT_LOG_TABLE = "analytics.fact_log"

# 체크포인트 디렉터리
FACT_LOG_CHECKPOINT_DIR = "/data/log-etlm/spark_checkpoints/fact_log"


class ClickHouseStreamWriter:
    def __init__(self, foreach_writer=write_to_clickhouse):
        self._foreach_writer = foreach_writer

    def _write_stream(
        self,
        df: DataFrame,
        table_name: str,
        checkpoint_dir: str,
        output_mode: str = "append",
        deduplicate_keys: list[str] | None = None,
    ):
        def _foreach(batch_df: DataFrame, _batch_id: int):
            out_df = batch_df
            if deduplicate_keys:
                # 배치 내 중복을 제거해 ClickHouse 중복 기록을 줄인다.
                out_df = out_df.dropDuplicates(deduplicate_keys) 
            self._foreach_writer(out_df, table_name, batch_id=_batch_id)

        return (
            df.writeStream
            .outputMode(output_mode)
            .foreachBatch(_foreach)
            .option("checkpointLocation", checkpoint_dir)
            .start()
        )

    def write_fact_log_stream(self, df: DataFrame):
        return self._write_stream(df, FACT_LOG_TABLE, FACT_LOG_CHECKPOINT_DIR)
