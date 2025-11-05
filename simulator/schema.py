# -----------------------------------------------------------------------------
# 파일명 : simulator/schema.py
# 목적   : 최소 9-키 로그 스키마(ts, svc, lvl, rid, met, path, st, lat, evt)의
#         유효성 검증과 경량 보정(normalize)을 담당.
# 사용   :
#   ok, errs = validate(event)
#   if ok:
#       event = normalize(event)
#   else:
#       -> DLQ로 보내거나, 로그로만 남기고 폐기
# -----------------------------------------------------------------------------

from __future__ import annotations
from typing import Dict, Any, List, Tuple
from datetime import datetime, timezone
import uuid

# 필수 키 집합(최소 스키마)
REQUIRED_KEYS = {"ts", "svc", "lvl", "rid", "met", "path", "st", "lat", "evt"}

# 허용 목록
ALLOWED_LVLS = {"I", "E"}                      # Info / Error
ALLOWED_SERVICES = {"auth", "order", "payment", "notify"}
ALLOWED_METHODS = {"GET", "POST", "PUT", "PATCH", "DELETE"}

# 간단한 타입 기대치
# - 엄격한 스키마(예: pydantic) 대신 가벼운 런타임 체크만 수행
EXPECTED_TYPES = {
    "ts": str,
    "svc": str,
    "lvl": str,
    "rid": str,
    "met": str,
    "path": str,
    "st": (int, float),   # 일부 프레임워크가 float로 줄 때를 대비
    "lat": (int, float),
    "evt": str,
}


def _is_iso8601_utc_z(ts: str) -> bool:
    """
    "YYYY-mm-ddTHH:MM:SSZ" 형태의 UTC ISO 포맷인지 간단히 검사.
    (밀리초/오프셋 변형은 허용하지 않음)
    """
    try:
        # 엄격히 'Z' 종결
        if not ts.endswith("Z"):
            return False
        datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ")
        return True
    except Exception:
        return False


def _now_utc_iso() -> str:
    """현재 UTC 시각을 '...Z' 형태로 반환."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _new_rid() -> str:
    """짧은 request id 생성."""
    return f"req_{uuid.uuid4().hex[:8]}"


def validate(event: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    이벤트가 최소 9-키 스키마에 맞는지 검사.

    Returns:
        (ok, errors):
          ok: True/False
          errors: 검증 실패 사유 목록
    """
    errors: List[str] = []

    # 1) 키 존재
    missing = REQUIRED_KEYS - set(event.keys())
    if missing:
        errors.append(f"missing keys: {sorted(missing)}")

    # 2) 타입 검사
    for k, t in EXPECTED_TYPES.items():
        if k in event and not isinstance(event[k], t):
            errors.append(f"invalid type for '{k}': got {type(event[k]).__name__}, expect {t}")

    # 3) 값 범위/형식 검사
    if "lvl" in event and event["lvl"] not in ALLOWED_LVLS:
        errors.append(f"invalid lvl: {event.get('lvl')}")

    if "svc" in event and event["svc"] not in ALLOWED_SERVICES:
        errors.append(f"invalid svc: {event.get('svc')}")

    if "met" in event and event["met"] not in ALLOWED_METHODS:
        errors.append(f"invalid met: {event.get('met')}")

    if "path" in event and (not isinstance(event["path"], str) or not event["path"].startswith("/")):
        errors.append("invalid path (must start with '/')")

    if "st" in event:
        try:
            st = int(event["st"])
            if st < 100 or st > 599:
                errors.append(f"invalid st range: {st}")
        except Exception:
            errors.append(f"invalid st (not int-like): {event['st']}")

    if "lat" in event:
        try:
            lat = float(event["lat"])
            if lat < 0:
                errors.append(f"invalid lat negative: {lat}")
        except Exception:
            errors.append(f"invalid lat (not float-like): {event['lat']}")

    if "ts" in event and not _is_iso8601_utc_z(str(event["ts"])):
        errors.append("invalid ts format (expect ISO8601 UTC with 'Z')")

    return (len(errors) == 0), errors


def normalize(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    이벤트를 보정한다(가능한 최소한의 안전 보정).
    - 누락된 키를 기본값으로 채움(최소 스키마)
    - 숫자 필드는 형 변환
    - ts 포맷이 이상하면 현재 시각으로 대체
    - 메서드/레벨/서비스가 허용 범위가 아니면 가장 가까운 기본값으로 치환

    주의:
    - normalize는 '치유' 목적이다. 중요한 모니터링에서는 validate의 에러를
      근본적으로 해결하는 게 맞다.
    """
    out = dict(event)  # 얕은 복사

    # 1) 필수 키 기본값
    if "ts" not in out or not _is_iso8601_utc_z(str(out.get("ts"))):
        out["ts"] = _now_utc_iso()
    if "svc" not in out or out["svc"] not in ALLOWED_SERVICES:
        out["svc"] = "auth"  # 임시 기본
    if "lvl" not in out or out["lvl"] not in ALLOWED_LVLS:
        out["lvl"] = "I"
    if "rid" not in out or not isinstance(out["rid"], str) or not out["rid"]:
        out["rid"] = _new_rid()
    if "met" not in out or out["met"] not in ALLOWED_METHODS:
        out["met"] = "GET"
    if "path" not in out or not isinstance(out["path"], str) or not out["path"].startswith("/"):
        out["path"] = "/"
    if "evt" not in out or not isinstance(out["evt"], str) or not out["evt"]:
        out["evt"] = "Unknown"

    # 2) 수치형 강제 변환
    try:
        out["st"] = int(out.get("st", 200))
    except Exception:
        out["st"] = 200
    try:
        out["lat"] = float(out.get("lat", 0.0))
    except Exception:
        out["lat"] = 0.0

    # 범위 보정
    if out["st"] < 100 or out["st"] > 599:
        out["st"] = 500
    if out["lat"] < 0:
        out["lat"] = 0.0

    return out
