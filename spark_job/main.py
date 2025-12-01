# spark_job/main.py
# spark-submit 진입점
# Kafka logs.* 토픽에서 데이터를 읽고 콘솔로 출력한다.

from pyspark.sql import SparkSession, types as T, functions as F
from pyspark.sql.functions import from_json, col

from .fact.fact_log import parse_fact_log
from .warehouse.writer import write_fact_log_stream

def main() -> None:
    # 1) Spark 세션 생성
    spark = SparkSession \
        .builder \
        .appName("LogForge_Spark_Job") \
        .config(
            "spark.jars.packages", 
            ",".join([
                "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1",
                "com.clickhouse:clickhouse-jdbc:0.4.6",
            ]),
        ) \
        .getOrCreate()

    # 2) Kafka logs.* 토픽에서 스트리밍 데이터 읽기
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribePattern", "logs.*") \
        .option("startingOffsets", "latest")  \
        .option("maxOffsetsPerTrigger", "1000") \
        .load()

    # 3) Kafka raw DF → fact_log 스키마로 파싱
    fact_df = parse_fact_log(kafka_df)

    # 4) ClickHouse analytics.fact_log로 스트리밍 적재
    query = write_fact_log_stream(fact_df)

    query.awaitTermination()

    # 어느 하나라도 죽으면 리턴
    # spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
