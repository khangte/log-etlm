from __future__ import annotations

def record_clickhouse_write(success: bool, rows: int) -> None:
    """이번 배치의 성공 여부와 성공률(행 기준)만 로그로 출력."""
    success_rows = rows if success else 0
    rate = (success_rows / rows * 100) if rows else 100.0
    status = "성공" if success else "실패"
    print(
        "[📊 ClickHouse] Batch %s | rows=%d success_rate=%.2f%%"
        % (status, rows, rate)
    )
