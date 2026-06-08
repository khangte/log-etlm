from __future__ import annotations

from pyspark.sql import DataFrame, functions as F

from ..schema import FACT_EVENT_COLUMNS


def normalize_event(good_df: DataFrame, *, store_raw_json: bool = False) -> DataFrame:
    """event_log 컬럼으로 표준화한다."""
    parsed = (
        good_df.select(
            # unified_ts_ms는 event_timestamp의 통합 소스
            F.coalesce(
                F.col("json.ts_ms"),
                F.col("json.timestamp_ms"), # 이전 형식과의 호환성을 위한 대체
                (F.col("kafka_received_at").cast("double") * F.lit(1000)).cast("long"), # JSON에 타임스탬프가 없는 경우의 대체
            ).alias("unified_ts_ms"),

            F.coalesce(F.col("json.service"), F.lit("unknown")).alias("service"),
            F.coalesce(F.col("json.api_group"), F.lit("unknown")).alias("api_group"),
            F.coalesce(
                F.col("json.event_name"),
                F.lit("unknown"),
            ).alias("event_name"),
            F.coalesce(
                F.col("json.result"),
                F.when(F.col("json.status_code") >= 400, F.lit("fail"))
                 .when(F.col("json.status_code").isNotNull(), F.lit("success")),
                F.lit("unknown"),
            ).alias("result"),
            F.coalesce(F.col("json.level"), F.lit("INFO")).alias("level"),
            F.coalesce(F.col("json.request_id"), F.lit("unknown")).alias("request_id"),
            F.coalesce(
                F.col("json.event_id"),
                F.when(
                    F.col("topic").isNotNull()
                    & F.col("partition").isNotNull()
                    & F.col("offset").isNotNull(),
                    F.concat_ws(
                        "-",
                        F.col("topic"),
                        F.col("partition").cast("string"),
                        F.col("offset").cast("string"),
                    ),
                ),
                F.lit("unknown"),
            ).alias("event_id"),
            F.coalesce(F.col("json.method"), F.lit("")).alias("method"),
            F.coalesce(
                F.col("json.route_template"),
                F.col("json.path"),
                F.lit(""),
            ).alias("route_template"),
            F.coalesce(
                F.col("json.path"),
                F.lit(""),
            ).alias("path"),
            F.coalesce(
                F.col("json.status_code"),
                F.when(F.col("json.result") == "fail", F.lit(500))
                 .when(F.col("json.result") == "success", F.lit(200)),
                F.lit(0),
            ).alias("status_code"),
            F.col("json.duration_ms").cast("int").alias("duration_ms"),
            F.col("json.user_id").alias("user_id"),
            F.col("json.order_id").alias("order_id"),
            F.col("json.payment_id").alias("payment_id"),
            F.col("json.reason_code").alias("reason_code"),
            F.col("json.product_id").alias("product_id"),
            F.col("json.amount").alias("amount"),
            F.col("topic"),
            F.col("partition").cast("int").alias("kafka_partition"),
            F.col("offset").cast("long").alias("kafka_offset"),
            F.col("kafka_received_at"),
            F.col("spark_received_at"),
            (F.col("raw_json") if store_raw_json else F.lit("")).alias("raw_json"),
        )
        .withColumn(
            "spark_processed_at",
            F.lit(None).cast("timestamp"),
        )
        .withColumn(
            "event_timestamp",
            F.to_timestamp((F.col("unified_ts_ms") / F.lit(1000)).cast("double")),
        )
    )

    return parsed.select(*FACT_EVENT_COLUMNS)
