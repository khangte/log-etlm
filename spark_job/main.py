# spark_job/main.py
# spark-submit 진입점

from __future__ import annotations

import os
from pyspark.sql.streaming import StreamingQueryException

from .stream_ingest import start_event_ingest_streams
from .spark import build_streaming_spark


def run_event_ingest() -> None:
    """Spark 세션을 구성하고 이벤트 ingest 스트림을 실행한다."""
    spark = None
    try:
        spark = build_streaming_spark(master=os.getenv("SPARK_MASTER_URL"))
        spark.sparkContext.setLogLevel("INFO")

        start_event_ingest_streams(spark)

        try:
            spark.streams.awaitAnyTermination()
        except StreamingQueryException as exc:
            # 드라이버 종료 원인 파악을 위해 전체 예외 메시지 출력
            print(f"[❌ 스트리밍 쿼리 예외] {exc}")
            raise

    except Exception as exc:
        print(f"[❌ SparkSession] 예기치 않은 오류: {exc}")
        raise

    finally:
        if spark:
            print("[ℹ️ SparkSession] 세션 종료.")
            spark.stop()


if __name__ == "__main__":
    run_event_ingest()
