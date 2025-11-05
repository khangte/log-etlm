# -----------------------------------------------------------------------------
# 파일명 : simulator/core/config.py
# 목적   : 시뮬레이터의 정적 리소스 경로와 로더 (프로파일/라우트)
# 사용   : runner 등에서 load_profile(), load_routes() 호출
# 주의   : Kafka 설정은 simulator/core/kafka.py로 분리됨
# -----------------------------------------------------------------------------

from __future__ import annotations
from typing import Dict, Any
from pathlib import Path
import yaml

# ===== 리소스 파일 경로 =====
ROOT_DIR = Path(__file__).resolve().parents[2]   # repo 루트
TEMPLATES_DIR = ROOT_DIR / "simulator" / "templates"
PROFILES_DIR  = ROOT_DIR / "simulator" / "profiles"

ROUTES_FILE = TEMPLATES_DIR / "routes.yml"

def load_profile(profile_path: str | Path) -> Dict[str, Any]:
    """
    YAML 프로파일을 로드한다.

    Args:
        profile_path: profiles/*.yaml 경로

    Returns:
        Dict[str, Any]: duration_sec, rps, mix, error_rate, bursts, time_weights 등 포함
    """
    path = Path(profile_path)
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data

def load_routes() -> Dict[str, Any]:
    """
    routes.yml을 로드하여 서비스별 라우트 리스트 맵을 반환한다.

    Returns:
        Dict[str, Any]: {"auth": [...], "order":[...], "payment":[...], "notify":[...]}
    """
    with ROUTES_FILE.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data.get("routes", {})
