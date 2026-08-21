# 파일명 : spark_job/stream_ingest.py
# 목적   : Kafka logs.* 토픽에서 데이터를 읽고 ClickHouse로 적재한다.

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

from pyspark.sql import SparkSession

from .fact.parsers.event_log import parse_event_log
from .fact.settings import get_fact_stream_settings
from .fact.writers.fact_writer import write_event_log_stream
from .stream_checkpoint import maybe_reset_checkpoint
from .stream_settings import get_stream_ingest_settings
from .stream_kafka import KafkaStreamBuilder


def start_event_ingest_streams(spark: SparkSession) -> None:
    """스트리밍 적재 작업을 시작한다."""
    settings = get_stream_ingest_settings()
    fact_settings = get_fact_stream_settings()
    kafka_builder = KafkaStreamBuilder(settings, fact_settings)

    maybe_reset_checkpoint(
        fact_settings.checkpoint_dir,
        enabled=settings.reset_checkpoint_on_start,
    )

    event_kafka_df = kafka_builder.build_kafka_stream(spark, settings.fact_topics)
    event_df = parse_event_log(event_kafka_df)

    write_event_log_stream(event_df)
