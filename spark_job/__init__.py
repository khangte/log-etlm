"""spark_job 패키지 초기화를 담당한다."""

from __future__ import annotations

import sys
from pathlib import Path


def _ensure_project_root() -> None:
    """프로젝트 루트 경로를 sys.path에 추가한다."""
    root = Path(__file__).resolve().parents[1]
    root_str = str(root)
    if root_str not in sys.path:
        sys.path.insert(0, root_str)


_ensure_project_root()
