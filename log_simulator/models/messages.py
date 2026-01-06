# -----------------------------------------------------------------------------
# 파일명 : log_simulator/models/messages.py
# 목적   : 파이프라인 공통 메시지 모델 정의
# -----------------------------------------------------------------------------

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class BatchMessage:
    service: str
    value: bytes
    key: Optional[bytes]
    replicate_error: bool
