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

    path_expr = F.coalesce(
        F.col("json.path"),
        F.col("json.route_template"),
        F.lit(""),
    )

    parsed = (
        good_df.select(
            F.coalesce(
                F.col("json.ts_ms"),
                F.col("json.ts"),
                F.col("json.timestamp_ms"),
            ).alias("event_ts_ms"),
            F.coalesce(F.col("json.service"), F.col("json.svc"), F.lit("unknown")).alias("service"),
            F.coalesce(F.col("json.api_group"), F.col("json.grp"), F.lit("unknown")).alias("api_group"),
            F.coalesce(
                F.col("json.event_name"),
                F.col("json.evt"),
                F.col("json.event"),
                F.lit("unknown"),
            ).alias("event_name"),
            F.coalesce(
                F.col("json.result"),
                F.col("json.res"),
                F.when(
                    F.coalesce(F.col("json.status_code"), F.col("json.st")) >= 400,
                    F.lit("fail"),
                ).when(
                    F.coalesce(F.col("json.status_code"), F.col("json.st")).isNotNull(),
                    F.lit("success"),
                ),
                F.lit("unknown"),
            ).alias("result"),
            F.coalesce(F.col("json.level"), F.col("json.lvl"), F.lit("INFO")).alias("level"),
            F.coalesce(F.col("json.request_id"), F.col("json.rid"), F.lit("unknown")).alias("request_id"),
            F.coalesce(
                F.col("json.event_id"),
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
            F.coalesce(F.col("json.method"), F.col("json.met"), F.lit("")).alias("method"),
            path_expr.alias("route_template"),
            path_expr.alias("path"),
            F.coalesce(
                F.col("json.status_code"),
                F.col("json.st"),
                F.when(F.coalesce(F.col("json.result"), F.col("json.res")) == "fail", F.lit(500))
                 .when(F.coalesce(F.col("json.result"), F.col("json.res")) == "success", F.lit(200)),
                F.lit(0),
            ).alias("status_code"),
            F.coalesce(F.col("json.duration_ms"), F.col("json.lat")).cast("int").alias("duration_ms"),
            F.coalesce(
                F.col("json.event_name"),
                F.col("json.evt"),
                F.col("json.event"),
                F.lit("unknown"),
            ).alias("event"),
            F.coalesce(F.col("json.user_id"), F.col("json.uid")).alias("user_id"),
            F.coalesce(F.col("json.order_id"), F.col("json.oid")).alias("order_id"),
            F.coalesce(F.col("json.payment_id"), F.col("json.pid")).alias("payment_id"),
            F.coalesce(F.col("json.reason_code"), F.col("json.rc")).alias("reason_code"),
            F.col("json.product_id").alias("product_id"),
            F.coalesce(F.col("json.amount"), F.col("json.amt")).cast("int").alias("amount"),
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
