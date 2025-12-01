# spark_job/warehouse/sink.py

from __future__ import annotations

from pyspark.sql import DataFrame

# ClickHouse 테이블 이름
FACT_LOG_TABLE = "analytics.fact_log"
# DIM_DATE_TABLE = "analytics.dim_date"
# DIM_TIME_TABLE = "analytics.dim_time"
# DIM_SERVICE_TABLE = "analytics.dim_service"
# DIM_STATUS_TABLE = "analytics.dim_status_code"

# 체크포인트 디렉터리
FACT_LOG_CHECKPOINT_DIR = "/data/spark_checkpoints/fact_log"
# DIM_DATE_CHECKPOINT_DIR = "/data/spark_checkpoints/dim_date"
# DIM_TIME_CHECKPOINT_DIR = "/data/spark_checkpoints/dim_time"
# DIM_SERVICE_CHECKPOINT_DIR = "/data/spark_checkpoints/dim_service"
# DIM_STATUS_CHECKPOINT_DIR = "/data/spark_checkpoints/dim_status"


from .sink import write_to_clickhouse

def write_fact_log_stream(fact_df: DataFrame):
    query = fact_df.writeStream \
        .outputMode("append") \
        .foreachBatch(lambda df, _: write_to_clickhouse(df, FACT_LOG_TABLE)) \
        .option("checkpointLocation", FACT_LOG_CHECKPOINT_DIR) \
        .start()
    return query


# def _write_stream(
#     df: DataFrame,
#     table_name: str,
#     checkpoint_dir: str,
#     mode: str = "append",
#     deduplicate_keys: list[str] | None = None,
# ):
#     """
#     공통 스트리밍 sink 헬퍼.
#     - foreachBatch 안에서 ClickHouse JDBC로 적재
#     - dedup_keys가 있으면, batch 내부에서 dropDuplicates(dedup_keys) 적용
#     """

#     def _sink(batch_df: DataFrame, batch_id: int):
#         # dim 쪽이라면 batch 안에서 dropDuplicates() 같은 것도 여기서 처리 가능
#         try:
#             out_df = batch_df

#             # 🔹 dim 쪽 등에서 키 기준 중복 제거 (마이크로배치 내)
#             if deduplicate_keys:
#                 out_df = out_df.dropDuplicates(deduplicate_keys)

#             print(f"[ClickHouse] Writing to {table_name}")
#             out_df.show(5, truncate=False)
#             (
#                 out_df.write
#                 .format("jdbc")
#                 .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
#                 .option("url", "jdbc:clickhouse://clickhouse:8123/analytics")
#                 .option("user", "log_sink")
#                 .option("password", "log_sink")
#                 .option("dbtable", table_name)
#                 .mode(mode)
#                 .save()
#             )
#         except Exception as e:
#             print(f"[ERROR] Failed writing {table_name} to ClickHouse: {e}")
#             import traceback
#             traceback.print_exc()

#     query = (
#         df.writeStream
#         .outputMode("append")
#         .foreachBatch(_sink)
#         .option("checkpointLocation", checkpoint_dir)
#         .start()
#     )

#     return query


# # def write_fact_log_stream(df: DataFrame):
# #     """
# #     fact_log 스트리밍 → ClickHouse
# #     """
# #     return _write_stream(
# #         df=df,
# #         table_name=FACT_LOG_TABLE,
# #         checkpoint_dir=FACT_LOG_CHECKPOINT_DIR,
# #         mode="append",
# #         deduplicate_keys=None,
# #     )


# def write_dim_date_stream(df: DataFrame):
#     """
#     dim_date 스트리밍 → ClickHouse
#     """
#     return _write_stream(
#         df=df,
#         table_name=DIM_DATE_TABLE,
#         checkpoint_dir=DIM_DATE_CHECKPOINT_DIR,
#         mode="append",
#         deduplicagte_keys=["date_key"],
#     )


# def write_dim_time_stream(df: DataFrame):
#     """
#     dim_time 스트리밍 → ClickHouse
#     """
#     return _write_stream(
#         df=df,
#         table_name=DIM_TIME_TABLE,
#         checkpoint_dir=DIM_TIME_CHECKPOINT_DIR,
#         mode="append",
#         deduplicate_keys=["time_key"],
#     )


# def write_dim_service_stream(df: DataFrame):
#     """
#     dim_service 스트리밍 → ClickHouse
#     """
#     return _write_stream(
#         df=df,
#         table_name=DIM_SERVICE_TABLE,
#         checkpoint_dir=DIM_SERVICE_CHECKPOINT_DIR,
#         mode="append",
#         deduplicate_keys=["service"],
#     )


# def write_dim_status_stream(df: DataFrame):
#     """
#     dim_status_code 스트리밍 → ClickHouse
#     """
#     return _write_stream(
#         df=df,
#         table_name=DIM_STATUS_TABLE,
#         checkpoint_dir=DIM_STATUS_CHECKPOINT_DIR,
#         mode="append",
#         deduplicate_keys=["status_code"],
#     )
