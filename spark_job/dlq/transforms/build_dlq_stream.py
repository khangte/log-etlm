from __future__ import annotations

from pyspark.sql import DataFrame

from ..parsers.parse_dlq import parse_dlq


def build_dlq_stream_df(dlq_source: DataFrame) -> DataFrame:
    """DLQ 스트림용 데이터프레임을 생성한다."""
    return parse_dlq(dlq_source)
