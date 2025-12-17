# spark_job/main.py
# spark-submit 진입점
# Kafka logs.* 토픽에서 데이터를 읽고 콘솔로 출력한다.

import os
import shutil
import time
from pyspark.sql import SparkSession, types as T, functions as F
from pyspark.sql.functions import from_json, col
from pyspark.sql.streaming import StreamingQueryException

from .fact.fact_log import parse_fact_log
from .warehouse.writer import ClickHouseStreamWriter, FACT_LOG_CHECKPOINT_DIR

writer = ClickHouseStreamWriter()

def _maybe_reset_checkpoint(checkpoint_dir: str) -> None:
    """
    Spark Structured Streaming 체크포인트가 깨졌을 때(예: 컨테이너 강제 종료),
    실시간 처리를 우선하는 모드에서는 체크포인트를 초기화하고 latest부터 다시 시작한다.
    """
    enabled = os.getenv("SPARK_RESET_CHECKPOINT_ON_START", "false").strip().lower() in ("1", "true", "yes", "y")
    if not enabled:
        return
    if not os.path.exists(checkpoint_dir):
        return

    ts = time.strftime("%Y%m%d-%H%M%S")
    backup = f"{checkpoint_dir}.bak.{ts}"
    print(f"[⚠️ checkpoint] reset enabled: move {checkpoint_dir} -> {backup}")
    shutil.move(checkpoint_dir, backup)
    os.makedirs(checkpoint_dir, exist_ok=True)


def main() -> None:
    spark = None
    try:
        # 1) Spark 세션 생성
        spark = SparkSession \
            .builder \
            .master(os.getenv('SPARK_MASTER_URL')) \
            .appName("LogForge_Spark_Job") \
            .config(
                "spark.jars.packages", 
                ",".join([
                    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1",
                    "com.clickhouse:clickhouse-jdbc:0.4.6",
                ]),
            ) \
            .config("spark.driver.bindAddress", "0.0.0.0") \
            .config("spark.ui.enabled", "true") \
            .config("spark.ui.port", "4040") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("INFO")

        # 체크포인트 손상 시(예: Incomplete log file) 실시간 모드에서 자동 초기화 옵션
        _maybe_reset_checkpoint(FACT_LOG_CHECKPOINT_DIR)
	 
        # 2) Kafka logs.* 토픽에서 스트리밍 데이터 읽기
        # 목표 처리량이 10k RPS라면, (배치 주기 dt) 기준으로 대략 maxOffsetsPerTrigger ~= 10000 * dt 로 잡아야 한다.
        # 기본값은 25만(예: dt=25s일 때 약 10k RPS 수준)으로 두고, 환경변수로 조절한다.
        max_offsets_per_trigger = os.getenv("SPARK_MAX_OFFSETS_PER_TRIGGER", "250000")
        kafka_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP")) \
            .option("subscribePattern", "logs.*") \
            .option("startingOffsets", "latest")  \
            .option("failOnDataLoss", "false") \
            .option("maxOffsetsPerTrigger", max_offsets_per_trigger) \
            .load()

        # 3) Kafka raw DF → fact_log 스키마로 파싱
        fact_df = parse_fact_log(kafka_df)

        # 4) ClickHouse analytics.fact_log로 스트리밍 적재
        query = writer.write_fact_log_stream(fact_df)

        try:
            query.awaitTermination()
        except StreamingQueryException as exc:
            # 드라이버 종료 원인 파악을 위해 전체 예외 메시지 출력
            print(f"[❌ StreamingQueryException] {exc}")
            raise

    except Exception as exc:
        print(f"[❌ SparkSession] Unexpected failure: {exc}")
        raise
        
    finally:
        if spark:
            print("[ℹ️ SparkSession] Stopping Spark session.")
            spark.stop()

    # 어느 하나라도 죽으면 리턴
    # spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
