# 파일명 : spark_job/stream_ingest.py
# 목적   : Kafka logs.* 토픽에서 데이터를 읽고 ClickHouse로 적재한다.

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

from pyspark.sql import SparkSession

from .dlq.parsers.parse_dlq import parse_dlq
from .dlq.settings import get_dlq_kafka_settings, get_dlq_stream_settings
from .dlq.writers.dlq_writer import ClickHouseDlqWriter
from .dlq.writers.kafka_writer import KafkaDlqWriter
from .fact.parsers.event_log import parse_event_log_with_errors
from .fact.settings import get_fact_stream_settings
from .fact.writers.fact_writer import ClickHouseFactWriter
from .stream_checkpoint import maybe_reset_checkpoint
from .stream_settings import get_stream_ingest_settings
from .stream_kafka import KafkaStreamBuilder


def start_event_ingest_streams(spark: SparkSession) -> None:
    """스트리밍 적재 작업을 시작한다."""
    settings = get_stream_ingest_settings()
    fact_settings = get_fact_stream_settings()
    kafka_builder = KafkaStreamBuilder(settings, fact_settings)
    fact_writer = ClickHouseFactWriter(fact_settings)
    dlq_writer = ClickHouseDlqWriter(get_dlq_stream_settings())
    dlq_kafka_writer = KafkaDlqWriter(get_dlq_kafka_settings())

    maybe_reset_checkpoint(
        fact_settings.checkpoint_dir,
        enabled=settings.reset_checkpoint_on_start,
    )

    event_kafka_df = kafka_builder.build_kafka_stream(spark, settings.fact_topics)
    event_df, bad_df = parse_event_log_with_errors(
        event_kafka_df,
        store_raw_json=fact_settings.store_raw_json,
        build_bad_df=settings.enable_dlq_stream,
    )

    fact_writer.write_event_log_stream(event_df)

    if settings.enable_dlq_stream:
        if bad_df is None:
            raise ValueError("DLQ stream enabled but bad_df is missing")
        dlq_topic = (settings.dlq_topic or "").strip()
        if not dlq_topic:
            raise ValueError("SPARK_DLQ_TOPIC is required when DLQ stream is enabled")
        dlq_kafka_writer.write_dlq_kafka_stream(bad_df, topic=dlq_topic)
        dlq_source = kafka_builder.build_kafka_stream(spark, dlq_topic)
        dlq_writer.write_dlq_stream(parse_dlq(dlq_source))
    else:
        logger.info("[INFO] DLQ 스트림 비활성화: bad_df는 저장하지 않음")
