from pyspark.sql import types as T

CLICKHOUSE_DB: str = "analytics"

# -----------------------------------------------------------------------------
# 1) Kafka value(JSON) 스키마
#    - simulator v2 이벤트 필드 + 레거시 필드 동시 수용
#    - from_json(col("value"), log_value_schema) 에서 사용
# -----------------------------------------------------------------------------

log_value_schema: T.StructType = T.StructType(
    [
        # v2 축약 필드(simulator 최신 포맷)
        T.StructField("eid", T.StringType(),  True),
        T.StructField("evt", T.StringType(),  True),
        T.StructField("ts",  T.LongType(),    True),
        T.StructField("svc", T.StringType(),  True),
        T.StructField("rid", T.StringType(),  True),
        T.StructField("met", T.StringType(),  True),
        T.StructField("path", T.StringType(), True),
        T.StructField("st",  T.IntegerType(), True),
        T.StructField("lat", T.IntegerType(), True),
        T.StructField("res", T.StringType(),  True),
        T.StructField("uid", T.StringType(),  True),
        T.StructField("oid", T.StringType(),  True),
        T.StructField("pid", T.StringType(),  True),
        T.StructField("rc",  T.StringType(),  True),
        T.StructField("grp", T.StringType(),  True),
        T.StructField("amt", T.DoubleType(),  True),
        T.StructField("lvl", T.StringType(),  True),

        # 레거시 호환 필드(기존 이벤트 포맷)
        T.StructField("timestamp_ms",      T.LongType(),    True),
        T.StructField("level",             T.StringType(),  True),
        T.StructField("event",             T.StringType(),  True),
        T.StructField("product_id",        T.IntegerType(), True),
    ]
)


# -----------------------------------------------------------------------------
# 2) ClickHouse analytics.fact_event 타겟 스키마
#    - 여기서는 타입/이름을 약간 정규화해서 사용
#    - Spark DF는 FACT_EVENT_COLUMNS에 있는 컬럼만 적재하고,
#      stored_ts는 ClickHouse에서 채운다(e2e_ms/sink_ms는 MV에서 계산).
# -----------------------------------------------------------------------------

fact_event_schema: T.StructType = T.StructType(
    [
        # 시간 관련
        T.StructField("event_ts",     T.TimestampType(), False),  # 발생 시각(UTC)
        T.StructField("ingest_ts",    T.TimestampType(), False),  # Kafka 적재 시각(UTC)
        T.StructField("processed_ts", T.TimestampType(), False),  # Spark 처리 시각(UTC)
        T.StructField("stored_ts",    T.TimestampType(), False),  # ClickHouse 저장 시각(UTC)
        T.StructField("ingest_ms",    T.IntegerType(), True),     # event->ingest 지연(ms, Spark)
        T.StructField("process_ms",   T.IntegerType(), True),     # ingest->process 지연(ms, Spark)

        # 공통 메타 정보
        T.StructField("service",     T.StringType(), False),
        T.StructField("api_group",   T.StringType(), False),
        T.StructField("event_name",  T.StringType(), False),
        T.StructField("result",      T.StringType(), False),
        T.StructField("request_id",  T.StringType(), False),
        T.StructField("event_id",    T.StringType(), False),
        T.StructField("method",      T.StringType(), True),
        T.StructField("route_template", T.StringType(), True),
        T.StructField("path",        T.StringType(), True),
        T.StructField("status_code", T.IntegerType(), True),
        T.StructField("duration_ms", T.IntegerType(), True),
        T.StructField("level",       T.StringType(), True),
        T.StructField("event",       T.StringType(),  True),  # 레거시 호환

        # 비즈니스 필드(서비스별로 있을 수도/없을 수도 있음)
        T.StructField("user_id",           T.StringType(),  True),
        T.StructField("order_id",          T.StringType(),  True),
        T.StructField("payment_id",        T.StringType(),  True),
        T.StructField("reason_code",       T.StringType(),  True),
        T.StructField("product_id",        T.IntegerType(), True),
        T.StructField("amount",            T.IntegerType(), True),

        # 인프라/추적용
        T.StructField("topic",          T.StringType(), True),   # Kafka topic
        T.StructField("kafka_partition", T.IntegerType(), True),
        T.StructField("kafka_offset",    T.LongType(), True),

        # 원본 백업
        T.StructField("raw_json", T.StringType(), False),
    ]
)


# -----------------------------------------------------------------------------
# 3) 공통 컬럼 순서 (select 순서 강제 등에서 사용)
# -----------------------------------------------------------------------------

FACT_EVENT_COLUMNS: list[str] = [
    # 1) 시간
    "event_ts",
    "ingest_ts",
    "processed_ts",
    "ingest_ms",
    "process_ms",

    # 2) 식별자 / 상관관계
    "event_id",
    "request_id",

    # 3) 분류 메타
    "service",
    "api_group",
    "event_name",
    "result",
    "level",
    "event",

    # 4) HTTP/요청 정보
    "method",
    "route_template",
    "path",
    "status_code",
    "duration_ms",

    # 5) 비즈니스 필드
    "user_id",
    "order_id",
    "payment_id",
    "reason_code",
    "product_id",
    "amount",

    # 6) Kafka 메타(재처리/포렌식 핵심)
    "topic",
    "kafka_partition",
    "kafka_offset",

    # 7) 원문
    "raw_json",
]
