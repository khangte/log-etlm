# 파일명 : spark_job/main.py
# 목적   : spark-submit 진입점

from __future__ import annotations

import logging
import os
from pyspark.sql.streaming import StreamingQueryException

from common.get_env import get_env_str
from spark_job.stream_ingest import start_event_ingest_streams
from spark_job.spark import build_streaming_spark

logger = logging.getLogger(__name__)


def run_event_ingest() -> None:
    """Spark 스트리밍 적재 작업을 실행한다."""
    spark = None
    try:
        master_url = get_env_str(os.environ, "SPARK_MASTER_URL")
        spark = build_streaming_spark(master=master_url)
        spark.sparkContext.setLogLevel("INFO")

        start_event_ingest_streams(spark)

        try:
            spark.streams.awaitAnyTermination()
        except StreamingQueryException as exc:
            # 드라이버 종료 원인 파악을 위해 전체 예외 메시지 출력
            logger.error("[ERROR] 스트리밍 쿼리 예외: %s", exc)
            raise

    except Exception as exc:
        logger.error("[ERROR] SparkSession 예기치 않은 오류: %s", exc)
        raise

    finally:
        if spark:
            logger.info("[INFO] SparkSession 세션 종료.")
            spark.stop()


if __name__ == "__main__":
    run_event_ingest()
