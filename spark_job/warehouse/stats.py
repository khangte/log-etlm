from __future__ import annotations

def record_clickhouse_write(success: bool, rows: int | None) -> None:
    """이번 배치의 성공 여부를 로그로 출력(행 수/성공률은 선택)."""
    status = "성공" if success else "실패"

    if rows is None:
        return

    success_rows = rows if success else 0
    rate = (success_rows / rows * 100) if rows else 100.0
    print("[📊 ClickHouse] Batch %s | rows=%d success_rate=%.2f%%" % (status, rows, rate))
