# -----------------------------------------------------------------------------
# 파일명 : log_simulator/models/messages.py
# 목적   : 파이프라인 공통 메시지 모델 정의
# -----------------------------------------------------------------------------

from __future__ import annotations

from dataclasses import dataclass
@dataclass(frozen=True)
class BatchMessage:
    service: str
    value: bytes
    key: bytes | None
    replicate_error: bool
