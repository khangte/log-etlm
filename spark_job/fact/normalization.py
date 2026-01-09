from __future__ import annotations

from pyspark.sql import DataFrame, functions as F

from .schema import FACT_EVENT_COLUMNS


def normalize_event(good_df: DataFrame, *, store_raw_json: bool = False) -> DataFrame:
    """
    parsed struct -> fact_event 컬럼 표준화.
    """
    def _ms_diff(end_col: str, start_col: str) -> F.Column:
        end_ts = F.col(end_col)
        start_ts = F.col(start_col)
        diff_ms = (end_ts.cast("double") - start_ts.cast("double")) * F.lit(1000)
        return (
            F.when(end_ts.isNull() | start_ts.isNull(), F.lit(None))
            .otherwise(F.greatest(diff_ms, F.lit(0.0)))
            .cast("int")
        )

    parsed = (
        good_df.select(
            F.col("json.ts").alias("event_ts_ms"),
            F.coalesce(F.col("json.svc"), F.lit("unknown")).alias("service"),
            F.coalesce(F.col("json.evt"), F.lit("unknown")).alias("event_name"),
            F.coalesce(F.col("json.res"), F.lit("unknown")).alias("result"),
            F.coalesce(F.col("json.rid"), F.lit("unknown")).alias("request_id"),
            F.coalesce(
                F.col("json.eid"),
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
                F.sha2(F.col("raw_json"), 256),
                F.lit("unknown"),
            ).alias("event_id"),
            F.col("json.uid").alias("user_id"),
            F.col("json.oid").alias("order_id"),
            F.col("json.pid").alias("payment_id"),
            F.col("json.rc").alias("reason_code"),
            F.col("json.product_id").alias("product_id"),
            F.col("json.amt").cast("int").alias("amount"),
            F.col("topic"),
            F.col("partition").cast("int").alias("kafka_partition"),
            F.col("offset").cast("long").alias("kafka_offset"),
            F.col("kafka_ts"),
            (F.col("raw_json") if store_raw_json else F.lit("")).alias("raw_json"),
        )
        .withColumn(
            "event_ts_ms",
            F.coalesce(
                F.col("event_ts_ms"),
                (F.col("kafka_ts").cast("double") * F.lit(1000)).cast("long"),
            ),
        )
        .withColumn(
            "ingest_ts",
            F.col("kafka_ts"),
        )
        .withColumn(
            "processed_ts",
            F.current_timestamp(),
        )
        .withColumn(
            "event_ts",
            F.to_timestamp((F.col("event_ts_ms") / F.lit(1000)).cast("double")),
        )
        .withColumn(
            "ingest_ms",
            _ms_diff("ingest_ts", "event_ts"),
        )
        .withColumn(
            "process_ms",
            _ms_diff("processed_ts", "ingest_ts"),
        )
    )

    return parsed.select(*FACT_EVENT_COLUMNS)
