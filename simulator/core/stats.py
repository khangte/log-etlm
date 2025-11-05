# -----------------------------------------------------------------------------
# 파일명 : simulator/core/stats.py
# 목적   : 전송 카운터 및 처리율(RPS) 측정/로그 보조
# 사용   : runner가 1초 루프에서 add_sent() 호출, N초 주기로 summary() 출력
# -----------------------------------------------------------------------------

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict
import time


@dataclass
class Stats:
    """
    전송 통계 관리.
    - total_sent: 전체 보낸 수
    - sent_by_svc: 서비스별 누적
    - started_at: 시작 시간(epoch)
    """
    started_at: float = field(default_factory=time.time)
    total_sent: int = 0
    sent_by_svc: Dict[str, int] = field(default_factory=dict)

    def add_sent(self, svc: str, n: int) -> None:
        """서비스별/전체 전송량을 누적."""
        self.total_sent += n
        self.sent_by_svc[svc] = self.sent_by_svc.get(svc, 0) + n

    def summary(self) -> str:
        """
        현재까지의 처리율 요약 문자열을 반환.
        - 평균 RPS = total_sent / elapsed
        - 서비스별 분포 표시
        """
        elapsed = max(1e-6, time.time() - self.started_at)
        avg_rps = self.total_sent / elapsed
        svc_parts = [f"{k}:{v}" for k, v in sorted(self.sent_by_svc.items())]
        return f"[stats] total={self.total_sent} avg_rps={avg_rps:.1f} by_svc=({', '.join(svc_parts)})"
