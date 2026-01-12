"""ClickHouse 전용 Spark JDBC 다이얼렉트 유틸."""

from __future__ import annotations

from pyspark.sql import types as T
from pyspark.sql.jdbc import JdbcDialect, JdbcDialects


class _ClickHouseDialect(JdbcDialect):
    """ClickHouse 고유 타입을 Spark SQL 타입으로 매핑."""

    def canHandle(self, url: str) -> bool:  # noqa: N802 (Spark interface)
        """ClickHouse JDBC URL인지 판별한다."""
        return bool(url) and url.lower().startswith("jdbc:clickhouse")

    def getCatalystType(self, sqlType, typeName, size, md):  # noqa: N802
        """ClickHouse 타입명을 Spark SQL 타입으로 변환한다."""
        if not typeName:
            return None

        lowered = typeName.lower()
        if lowered.startswith("datetime64"):
            return T.TimestampType()
        if lowered.startswith("datetime"):
            return T.TimestampType()

        return None


_REGISTERED = False


def register_clickhouse_dialect() -> None:
    """JVM 당 한 번만 커스텀 다이얼렉트를 등록."""
    global _REGISTERED
    if _REGISTERED:
        return

    JdbcDialects.registerDialect(_ClickHouseDialect())
    _REGISTERED = True


register_clickhouse_dialect()
