from __future__ import annotations

from pyspark.sql import DataFrame

from .build_dlq import build_dlq_df
from .parse_dlq import parse_dlq


def build_dlq_stream_df(dlq_source: DataFrame, bad_df: DataFrame) -> DataFrame:
    """
    파싱/검증 실패 레코드와 DLQ 토픽 레코드를 하나의 DLQ 스트림으로 합친다.
    """
    parse_error_dlq_df = build_dlq_df(bad_df)
    dlq_df = parse_dlq(dlq_source)
    return parse_error_dlq_df.unionByName(dlq_df)
