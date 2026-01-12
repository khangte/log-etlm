# -----------------------------------------------------------------------------
# file: spark_job/clickhouse/writer_base.py
# purpose: shared ClickHouse writer helpers for fact/dim/dlq
# -----------------------------------------------------------------------------

from __future__ import annotations

from pyspark.sql import DataFrame

from .sink import write_to_clickhouse


class ClickHouseStreamWriterBase:
    def __init__(self, foreach_writer=write_to_clickhouse):
        """스트리밍 foreachBatch에 사용할 writer를 설정한다."""
        self._foreach_writer = foreach_writer

    def write_stream(
        self,
        df: DataFrame,
        table_name: str,
        checkpoint_dir: str,
        *,
        output_mode: str = "append",
        query_name: str | None = None,
        deduplicate_keys: list[str] | None = None,
    ):
        """스트리밍 DataFrame을 ClickHouse로 적재한다."""
        def _foreach(batch_df: DataFrame, batch_id: int):
            """마이크로배치 단위로 중복 제거 후 쓰기를 수행한다."""
            out_df = batch_df
            if deduplicate_keys:
                # Drop duplicates per micro-batch to reduce ClickHouse duplicates.
                out_df = out_df.dropDuplicates(deduplicate_keys)
            self._foreach_writer(out_df, table_name, batch_id=batch_id)

        writer = (
            df.writeStream
            .outputMode(output_mode)
            .foreachBatch(_foreach)
            .option("checkpointLocation", checkpoint_dir)
        )
        if query_name:
            writer = writer.queryName(query_name)
        return writer.start()


class ClickHouseBatchWriterBase:
    def __init__(self, batch_writer=write_to_clickhouse):
        """배치 쓰기에 사용할 writer를 설정한다."""
        self._batch_writer = batch_writer

    def write_batch(
        self,
        df: DataFrame,
        table_name: str,
        *,
        deduplicate_keys: list[str] | None = None,
    ):
        """배치 DataFrame을 ClickHouse로 적재한다."""
        out_df = df
        if deduplicate_keys:
            out_df = out_df.dropDuplicates(deduplicate_keys)
        self._batch_writer(out_df, table_name)
