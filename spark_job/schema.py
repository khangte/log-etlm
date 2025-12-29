from pyspark.sql import types as T

CLICKHOUSE_DB: str = "analytics"

# -----------------------------------------------------------------------------
# 1) Kafka value(JSON) 스키마
#    - 기존에 사용하던 log_schema를 그대로 옮겨옴
#    - from_json(col("value"), log_value_schema) 에서 사용
# -----------------------------------------------------------------------------
log_value_schema: T.StructType = T.StructType(
    [
        # Spark ingest 성능을 위해 epoch(ms)만 전달
        T.StructField("timestamp_ms", T.LongType(),   True),
        T.StructField("service",     T.StringType(),  True),
        T.StructField("level",       T.StringType(),  True),
        T.StructField("request_id",  T.StringType(),  True),
        T.StructField("method",      T.StringType(),  True),
        T.StructField("path",        T.StringType(),  True),
        T.StructField("status_code", T.IntegerType(), True),
        T.StructField("event",       T.StringType(),  True),

        T.StructField("user_id",           T.StringType(),  True),
        T.StructField("notification_type", T.StringType(),  True),
        T.StructField("product_id",        T.IntegerType(), True),
        T.StructField("amount",            T.IntegerType(), True),
    ]
)


# -----------------------------------------------------------------------------
# 2) ClickHouse analytics.fact_log 타겟 스키마
#    - 여기서는 타입/이름을 약간 정규화해서 사용
#    - Spark DF가 이 스키마를 만족하도록 parsing 코드(fact/fact_log.py)를 작성
# -----------------------------------------------------------------------------
fact_log_schema: T.StructType = T.StructType(
    [
        # 시간 관련
        T.StructField("event_ts",     T.TimestampType(), False),  # 발생 시각(UTC)
        T.StructField("ingest_ts",    T.TimestampType(), False),  # Kafka 적재 시각(UTC)
        T.StructField("processed_ts", T.TimestampType(), False),  # Spark 처리 시각(UTC)

        # 공통 메타 정보
        T.StructField("service",     T.StringType(), False),
        T.StructField("level",       T.StringType(), True),
        T.StructField("request_id",  T.StringType(), True),
        T.StructField("method",      T.StringType(), True),
        T.StructField("path",        T.StringType(), True),
        T.StructField("status_code", T.IntegerType(), True),
        T.StructField("event",       T.StringType(),  True),

        # 비즈니스 필드(서비스별로 있을 수도/없을 수도 있음)
        T.StructField("user_id",           T.StringType(),  True),
        T.StructField("notification_type", T.StringType(),  True),
        T.StructField("product_id",        T.IntegerType(), True),
        T.StructField("amount",            T.IntegerType(), True),

        # 인프라/추적용
        T.StructField("topic",    T.StringType(), True),   # Kafka topic

        # 원본 백업
        T.StructField("raw_json", T.StringType(), False),
    ]
)


# -----------------------------------------------------------------------------
# 3) 공통 컬럼 순서 (select 순서 강제 등에서 사용)
# -----------------------------------------------------------------------------
FACT_LOG_COLUMNS: list[str] = [
    "event_ts",
    "ingest_ts",
    "processed_ts",
    "service",
    "level",
    "request_id",
    "method",
    "path",
    "status_code",
    "event",
    "user_id",
    "notification_type",
    "product_id",
    "amount",
    "topic",
    "raw_json",
]


# -----------------------------------------------------------------------------
# 4) Dimension 테이블 컬럼 순서
# -----------------------------------------------------------------------------

DIM_TIME_SCHEMA = T.StructType(
    [
        T.StructField("time_key",    T.IntegerType(), False),  # HHMMSS 형식 (예: 93015)
        T.StructField("hour",        T.IntegerType(), False),  # 0~23
        T.StructField("minute",      T.IntegerType(), False),  # 0~59
        T.StructField("second",      T.IntegerType(), False),  # 0~59
        T.StructField("time_of_day", T.StringType(),  False),  # dawn/morning/afternoon/evening
    ]
)

DIM_DATE_SCHEMA = T.StructType(
    [
        T.StructField("date",        T.DateType(),    False),
        T.StructField("year",        T.IntegerType(), False),
        T.StructField("month",       T.IntegerType(), False),
        T.StructField("day",         T.IntegerType(), False),
        T.StructField("week",        T.IntegerType(), False),
        T.StructField("day_of_week", T.IntegerType(), False),
        T.StructField("is_weekend",  T.IntegerType(), False),
    ]
)

DIM_SERVICE_SCHEMA = T.StructType(
    [
        T.StructField("service",       T.StringType(), False),
        T.StructField("service_group", T.StringType(), False),
        T.StructField("is_active",     T.IntegerType(), False),
        T.StructField("description",   T.StringType(), True),
    ]
)

DIM_STATUS_CODE_SCHEMA = T.StructType(
    [
        T.StructField("status_code",  T.IntegerType(), False),
        T.StructField("status_class", T.StringType(),  False),
        T.StructField("is_error",     T.IntegerType(), False),
        T.StructField("description",  T.StringType(),  True),
    ]
)

DIM_USER_SCHEMA = T.StructType(
    [
        T.StructField("user_id",     T.StringType(), False),
        T.StructField("is_active",   T.IntegerType(), False),
        T.StructField("description", T.StringType(), True),
    ]
)

DIM_DATE_COLUMNS: list[str] = [
    "date",
    "year",
    "month",
    "day",
    "week",
    "day_of_week",
    "is_weekend",
]

DIM_TIME_COLUMNS: list[str] = [
    "time_key",
    "hour",
    "minute",
    "second",
    "time_of_day",
]

DIM_SERVICE_COLUMNS: list[str] = [
    "service",
    "service_group",
    "is_active",
    "description",
]

DIM_STATUS_CODE_COLUMNS: list[str] = [
    "status_code",
    "status_class",
    "is_error",
    "description",
]

DIM_USER_COLUMNS: list[str] = [
    "user_id",
    "is_active",
    "description",
]
