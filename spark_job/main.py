# spark_job/main.py
# spark-submit 진입점
# Kafka logs.* 토픽에서 데이터를 읽고 콘솔로 출력한다.

from pyspark.sql import SparkSession, types as T, functions as F
from pyspark.sql.functions import from_json, col

def main():
    # 1) Spark 세션 생성
    spark = SparkSession \
        .builder \
        .appName("LogForge_Spark_Job") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1") \
        .getOrCreate()

    # 2) Kafka logs.* 토픽에서 스트리밍 데이터 읽기
    df_raw = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribePattern", "logs.*") \
        .option("startingOffsets", "latest")  \
        .option("maxOffsetsPerTrigger", "1000") \
        .load()

    # 3) key, value 둘 다 string으로 캐스팅
    df_casted = df_raw \
        .selectExpr(
            "CAST(key AS STRING) AS key",
            "CAST(value AS STRING) AS value",
            "topic",
            "partition",
            "offset",
            "timestamp",
        )

    # 4) JSON 스키마 정의 (시뮬레이터 로그 기준)
    log_schema = T.StructType(
        [
            T.StructField("ts", T.StringType(), True),
            T.StructField("svc", T.StringType(), True),
            T.StructField("lvl", T.StringType(), True),
            T.StructField("rid", T.StringType(), True),
            T.StructField("met", T.StringType(), True),
            T.StructField("path", T.StringType(), True),
            T.StructField("st", T.IntegerType(), True),
            T.StructField("lat", T.DoubleType(), True),
            T.StructField("evt", T.StringType(), True),
        ]
    )

    # 5) JSON → 칼럼으로 파싱
    df_parsed = df_casted \
        .withColumn("json", F.from_json("value", log_schema)) \
        .select(
            "topic",
            "partition",
            "offset",
            "timestamp",
            "key",
            F.col("json.ts").alias("ts"),
            F.col("json.svc").alias("svc"),
            F.col("json.lvl").alias("lvl"),
            F.col("json.rid").alias("rid"),
            F.col("json.met").alias("met"),
            F.col("json.path").alias("path"),
            F.col("json.st").alias("st"),
            F.col("json.lat").alias("lat"),
            F.col("json.evt").alias("evt"),
        )

    # 6) 콘솔로 출력하는 스트리밍 쿼리
    query = df_parsed \
        .writeStream \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", 20) \
        .option("checkpointLocation", "/data/spark_checkpoints/console_logs") \
        .outputMode("append") \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    main()
