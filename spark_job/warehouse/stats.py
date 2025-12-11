from __future__ import annotations

from dataclasses import dataclass


@dataclass
class _ClickHouseWriteStats:
    """간단한 ClickHouse 적재 성공률 추적기."""

    total_batches: int = 0
    success_batches: int = 0
    failure_batches: int = 0
    total_rows: int = 0
    success_rows: int = 0
    failure_rows: int = 0

    def record(self, success: bool, rows: int) -> None:
        self.total_batches += 1
        self.total_rows += rows

        if success:
            self.success_batches += 1
            self.success_rows += rows
        else:
            self.failure_batches += 1
            self.failure_rows += rows

    def row_success_pct(self) -> float:
        if not self.total_rows:
            return 0.0
        return (self.success_rows / self.total_rows) * 100

    def batch_success_pct(self) -> float:
        if not self.total_batches:
            return 0.0
        return (self.success_batches / self.total_batches) * 100


_WRITE_STATS = _ClickHouseWriteStats()


def _report_write_stats() -> None:
    row_rate = _WRITE_STATS.row_success_pct()
    batch_rate = _WRITE_STATS.batch_success_pct()
    print(
        "[📊 ClickHouse] Success rate | "
        f"rows={_WRITE_STATS.success_rows}/{_WRITE_STATS.total_rows} ({row_rate:.2f}%) "
        f"batches={_WRITE_STATS.success_batches}/{_WRITE_STATS.total_batches} ({batch_rate:.2f}%)"
    )


def record_clickhouse_write(success: bool, rows: int) -> None:
    """적재 결과를 기록하고 최신 성공률을 로그로 출력."""
    _WRITE_STATS.record(success, rows)
    _report_write_stats()
