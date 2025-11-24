# -----------------------------------------------------------------------------
# 파일명 : log_gateway/api/__init__.py
# 목적   : FastAPI 서브모듈(ingest/simulate) 라우터를 한 번에 노출
# 설명   : main.py 등에서 from log_gateway.api import ingest 형태로 사용할 수 있도록 함
# -----------------------------------------------------------------------------

from . import ingest, simulate

__all__ = ["ingest", "simulate"]
