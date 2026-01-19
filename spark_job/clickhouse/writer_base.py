# -----------------------------------------------------------------------------
# 파일명 : spark_job/clickhouse/writer_base.py
# 목적   : fact/dim/dlq 공용 ClickHouse writer 헬퍼
# -----------------------------------------------------------------------------

from __future__ import annotations

import time

from pyspark.sql import DataFrame

from ..batch_log import append_batch_log
from .sink import write_to_clickhouse


class ClickHouseStreamWriterBase:
    """스트리밍 데이터의 ClickHouse 적재를 담당한다."""

    def __init__(self, foreach_writer=write_to_clickhouse):
        """배치 처리 함수를 주입한다."""
        self._foreach_writer = foreach_writer

    def write_stream(
        self,
        df: DataFrame,
        table_name: str,
        checkpoint_dir: str,
        *,
        output_mode: str = "append",
        query_name: str | None = None,
        stream_name: str | None = None,
        deduplicate_keys: list[str] | None = None,
        skip_empty: bool = False,
        trigger_processing_time: str | None = None,
    ):
        """스트리밍 데이터를 ClickHouse로 적재한다."""
        resolved_stream = stream_name or query_name or table_name
        prefix_parts = [
            "[spark batch]",
            f"stream={resolved_stream}",
            f"table={table_name}",
        ]
        if query_name:
            prefix_parts.append(f"query={query_name}")
        prefix = " ".join(prefix_parts)

        def _foreach(batch_df: DataFrame, batch_id: int):
            """배치별 ClickHouse 쓰기와 타이밍 로그를 처리한다."""
            start_time = time.perf_counter()
            row_count = int(batch_df.count())
            row_line = f"{prefix} batch_id={batch_id} rows={row_count}"
            print(row_line)
            append_batch_log(row_line)
            if skip_empty and row_count == 0:
                elapsed = time.perf_counter() - start_time
                line = f"{prefix} batch_id={batch_id} empty=true duration={elapsed:.3f}s"
                print(line)
                append_batch_log(line)
                return
            out_df = batch_df
            if deduplicate_keys:
                # 마이크로 배치 단위 중복 제거로 적재 중복을 줄인다.
                out_df = out_df.dropDuplicates(deduplicate_keys)
            self._foreach_writer(out_df, table_name, batch_id=batch_id)
            elapsed = time.perf_counter() - start_time
            line = f"{prefix} batch_id={batch_id} duration={elapsed:.3f}s"
            print(line)
            append_batch_log(line)

        writer = (
            df.writeStream
            .outputMode(output_mode)
            .foreachBatch(_foreach)
            .option("checkpointLocation", checkpoint_dir)
        )
        if trigger_processing_time:
            writer = writer.trigger(processingTime=trigger_processing_time)
        if query_name:
            writer = writer.queryName(query_name)
        return writer.start()


class ClickHouseBatchWriterBase:
    """배치 데이터의 ClickHouse 적재를 담당한다."""

    def __init__(self, batch_writer=write_to_clickhouse):
        """배치 처리 함수를 주입한다."""
        self._batch_writer = batch_writer

    def write_batch(
        self,
        df: DataFrame,
        table_name: str,
        *,
        deduplicate_keys: list[str] | None = None,
    ):
        """배치 데이터를 ClickHouse로 적재한다."""
        out_df = df
        if deduplicate_keys:
            out_df = out_df.dropDuplicates(deduplicate_keys)
        self._batch_writer(out_df, table_name)
