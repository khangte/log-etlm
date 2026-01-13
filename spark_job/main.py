# spark_job/main.py
# spark-submit 진입점
# fact_event_stream, fact_event_dlq_stream을 동시에 실행한다.

from __future__ import annotations

import os
from pyspark.sql.streaming import StreamingQueryException

from .fact.jobs.fact_event_stream import start_fact_event_stream
from .dlq.jobs.fact_event_dlq_stream import start_fact_event_dlq_stream
from .progress_logger import start_progress_logger
from .spark import build_streaming_spark


def run_streaming_jobs() -> None:
    """스트리밍 쿼리를 기동하고 종료까지 대기한다."""
    spark = build_streaming_spark(master=os.getenv("SPARK_MASTER_URL"))
    spark.sparkContext.setLogLevel("INFO")

    fact_query = start_fact_event_stream(spark)
    dlq_query = start_fact_event_dlq_stream(spark)

    progress_enabled = os.getenv("SPARK_PROGRESS_LOG_ENABLED", "true").strip().lower() in (
        "1",
        "true",
        "yes",
        "y",
    )
    progress_log_path = os.getenv(
        "SPARK_PROGRESS_LOG_PATH",
        "/data/log-etlm/spark-events/spark_progress.log",
    )
    progress_interval = float(os.getenv("SPARK_PROGRESS_LOG_INTERVAL_SEC", "5"))

    progress_stop = None
    if progress_enabled:
        progress_queries = [fact_query]
        if dlq_query is not None:
            progress_queries.append(dlq_query)
        progress_stop, _ = start_progress_logger(
            progress_queries,
            interval_sec=max(progress_interval, 1.0),
            log_path=progress_log_path,
        )

    try:
        spark.streams.awaitAnyTermination()
    except StreamingQueryException as exc:
        print(f"[❌ 스트리밍 쿼리 예외] {exc}")
        raise
    finally:
        if progress_stop is not None:
            progress_stop.set()


if __name__ == "__main__":
    run_streaming_jobs()
