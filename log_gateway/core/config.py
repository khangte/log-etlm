# -----------------------------------------------------------------------------
# 파일명 : log_gateway/core/config.py
# 목적   : log_gateway 앱의 정적 리소스 경로(templates/profiles) 및 YAML 로더 제공
# 사용   : generator/API가 load_profile(), load_routes()로 시뮬레이션 설정을 읽어옴
# -----------------------------------------------------------------------------

from __future__ import annotations
from typing import Dict, Any
from pathlib import Path
import yaml

# ===== 리소스 파일 경로 =====
# config.py 위치 기준
THIS_FILE = Path(__file__).resolve()
ROOT_DIR = THIS_FILE.parents[2] # repo 루트
APP_DIR = THIS_FILE.parents[1] # 앱 루트: log_gateway/
TEMPLATES_DIR = ROOT_DIR / APP_DIR / "templates"
PROFILES_DIR  = ROOT_DIR / APP_DIR / "profiles"

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
