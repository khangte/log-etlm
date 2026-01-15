from __future__ import annotations

from pyspark.sql import DataFrame

from ..parsers.parse_dlq import parse_dlq


def build_dlq_stream_df(dlq_source: DataFrame) -> DataFrame:
    """
    Kafka logs.dlq 레코드를 DLQ 스트림으로 변환한다.
    """
    return parse_dlq(dlq_source)
