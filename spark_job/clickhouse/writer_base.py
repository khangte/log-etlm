# -----------------------------------------------------------------------------
# file: spark_job/clickhouse/writer_base.py
# purpose: shared ClickHouse writer helpers for fact/dim/dlq
# -----------------------------------------------------------------------------

from __future__ import annotations

import os
import time

from pyspark.sql import DataFrame

from .sink import write_to_clickhouse


def _append_batch_log(line: str) -> None:
    """배치 타이밍 로그를 파일에 추가한다."""
    log_path = os.getenv("SPARK_BATCH_TIMING_LOG_PATH", "").strip()
    if not log_path:
        return
    try:
        log_dir = os.path.dirname(log_path)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
        utc_ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        with open(log_path, "a", encoding="utf-8") as logfile:
            logfile.write(f"{utc_ts} {line}\n")
    except Exception as exc:
        print(f"[spark batch] log write failed: {exc}")


class ClickHouseStreamWriterBase:
    def __init__(self, foreach_writer=write_to_clickhouse):
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
        skip_empty: bool = False,
        trigger_processing_time: str | None = None,
    ):
        """Structured Streaming을 ClickHouse로 적재한다."""
        def _foreach(batch_df: DataFrame, batch_id: int):
            """배치별 ClickHouse 쓰기와 타이밍 로그를 처리한다."""
            start_time = time.perf_counter()
            if skip_empty and batch_df.rdd.isEmpty():
                elapsed = time.perf_counter() - start_time
                line = (
                    "[spark batch] "
                    f"table={table_name} batch_id={batch_id} empty=true duration={elapsed:.3f}s"
                )
                print(line)
                _append_batch_log(line)
                return
            out_df = batch_df
            if deduplicate_keys:
                # Drop duplicates per micro-batch to reduce ClickHouse duplicates.
                out_df = out_df.dropDuplicates(deduplicate_keys)
            self._foreach_writer(out_df, table_name, batch_id=batch_id)
            elapsed = time.perf_counter() - start_time
            line = (
                "[spark batch] "
                f"table={table_name} batch_id={batch_id} duration={elapsed:.3f}s"
            )
            print(line)
            _append_batch_log(line)

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
    def __init__(self, batch_writer=write_to_clickhouse):
        self._batch_writer = batch_writer

    def write_batch(
        self,
        df: DataFrame,
        table_name: str,
        *,
        deduplicate_keys: list[str] | None = None,
    ):
        """Batch DF를 ClickHouse로 적재한다."""
        out_df = df
        if deduplicate_keys:
            out_df = out_df.dropDuplicates(deduplicate_keys)
        self._batch_writer(out_df, table_name)
