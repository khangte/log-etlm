# -----------------------------------------------------------------------------
# 파일명 : simulator/cli/args.py
# 목적   : 시뮬레이터 실행을 위한 커맨드라인 인자 정의/파싱
# 사용   :
#   from simulator.cli.args import parse_args
#   ns = parse_args()
#   print(ns.profile, ns.routes, ns.duration, ns.rps, ns.dry_run, ...)
#
# 주요 옵션:
#   --profile         : profiles/*.yaml 경로 (필수 아님, 미지정 시 baseline.yaml 시도)
#   --routes          : templates/routes.yml 경로(미지정 시 기본값)
#   --duration        : 실행 시간(초), 프로파일 값 override
#   --rps             : 기본 RPS, 프로파일 값 override
#   --dry-run         : Kafka 미발행, stdout 샘플만
#   --no-error-repl   : 에러 복제발행 비활성화(기본은 활성)
#   --log-interval    : 통계 로그 간격(초)
#   --mix             : 서비스 비율 오버라이드(k=v,쉼표구분) 예: auth=0.2,order=0.5,payment=0.2,notify=0.1
#   --error-rate      : 에러율 오버라이드(k=v,쉼표구분 또는 숫자) 예: order=0.03,payment=0.05  /  0.02
#
# 팁:
#   - --mix, --error-rate는 전부 문자열로 받아 helper에서 dict/float로 변환해 전달.
#   - --profile 미지정 시 baseline.yaml을 우선 시도하고, 없으면 에러.
# -----------------------------------------------------------------------------

from __future__ import annotations
import argparse
from pathlib import Path
from typing import Dict, Any, Tuple, Union

from simulator.core.config import PROFILES_DIR, ROUTES_FILE


def _default_profile_path() -> Path:
    """
    기본 프로파일 경로를 반환한다.
    우선순위: PROFILES_DIR/baseline.yaml 존재 시 → 그 경로, 아니면 예외 발생.

    Returns:
        Path: 기본 프로파일 경로

    Raises:
        FileNotFoundError: 기본 프로파일이 없을 때
    """
    candidate = PROFILES_DIR / "baseline.yaml"
    if candidate.exists():
        return candidate
    raise FileNotFoundError(
        f"Default profile not found: {candidate}. "
        "Provide --profile with a valid YAML."
    )


def _parse_kv_map(text: str) -> Dict[str, float]:
    """
    'k=v,k=v' 형태의 문자열을 dict[str,float]로 변환한다.

    Args:
        text: 예) 'auth=0.2,order=0.4,payment=0.3,notify=0.1'

    Returns:
        Dict[str,float]: {'auth':0.2, 'order':0.4, ...}

    Raises:
        ValueError: 파싱 실패 시
    """
    if not text:
        return {}
    result: Dict[str, float] = {}
    parts = [p.strip() for p in text.split(",") if p.strip()]
    for p in parts:
        if "=" not in p:
            raise ValueError(f"Invalid pair '{p}'. Expected k=v.")
        k, v = p.split("=", 1)
        k = k.strip()
        try:
            result[k] = float(v.strip())
        except Exception as e:
            raise ValueError(f"Invalid float for '{k}': '{v}'") from e
    return result


def _parse_error_rate(text: str) -> Union[float, Dict[str, float]]:
    """
    error-rate 오버라이드 파서.
    - 단일 숫자: 전체 공통값.  예) '0.02'
    - k=v 목록: 서비스별.  예) 'auth=0.01,order=0.03'

    Args:
        text: 사용자 입력 문자열

    Returns:
        float | Dict[str,float]: 숫자 또는 서비스별 딕셔너리

    Raises:
        ValueError: 형식이 잘못된 경우
    """
    if not text:
        return {}
    # 숫자로 전체값 지정
    try:
        return float(text)
    except Exception:
        # k=v map으로 파싱 시도
        return _parse_kv_map(text)


def build_parser() -> argparse.ArgumentParser:
    """
    argparse.ArgumentParser 인스턴스를 구성해 반환한다.

    Returns:
        argparse.ArgumentParser: 시뮬레이터용 파서
    """
    p = argparse.ArgumentParser(
        prog="log-fore-simulator",
        description="Generate minimal 9-key logs and publish to Kafka (or dry-run).",
    )

    # 파일 경로
    p.add_argument(
        "--profile",
        type=Path,
        default=None,
        help="profiles/*.yaml 경로. 미지정 시 baseline.yaml을 시도.",
    )
    p.add_argument(
        "--routes",
        type=Path,
        default=ROUTES_FILE,
        help=f"templates/routes.yml 경로 (기본: {ROUTES_FILE})",
    )

    # 오버라이드(선택)
    p.add_argument(
        "--duration",
        type=int,
        default=None,
        help="실행 시간(초). 지정 시 프로파일 값을 오버라이드.",
    )
    p.add_argument(
        "--rps",
        type=int,
        default=None,
        help="기본 초당 생성 수. 지정 시 프로파일 값을 오버라이드.",
    )
    p.add_argument(
        "--mix",
        type=str,
        default=None,
        help="서비스 비율 오버라이드. 예) auth=0.2,order=0.4,payment=0.3,notify=0.1",
    )
    p.add_argument(
        "--error-rate",
        dest="error_rate",
        type=str,
        default=None,
        help="에러율 오버라이드(숫자 or k=v 목록). 예) 0.02  또는  auth=0.01,order=0.03",
    )

    # 실행 옵션
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Kafka 발행 없이 stdout 샘플만 출력.",
    )
    p.add_argument(
        "--no-error-repl",
        action="store_true",
        help="에러 복제발행 비활성화(기본은 활성).",
    )
    p.add_argument(
        "--log-interval",
        type=int,
        default=5,
        help="통계 로그 간격(초). 기본 5.",
    )

    return p


def normalize_args(ns: argparse.Namespace) -> Tuple[Path, Path, Dict[str, Any]]:
    """
    파싱 결과를 실행에 바로 쓰기 좋게 정규화한다.

    - profile 경로 확정(미지정 시 baseline.yaml 시도)
    - routes 경로 확정
    - overrides dict 구성(duration/rps/mix/error_rate/dry_run/replicate_error_to_topic/log_interval_sec)

    Args:
        ns: argparse.Namespace (build_parser().parse_args() 결과)

    Returns:
        (profile_path, routes_path, overrides) 튜플
    """
    # 프로파일 경로 확정
    profile_path: Path = ns.profile or _default_profile_path()

    # 라우트 경로 확정
    routes_path: Path = ns.routes

    # 오버라이드 구성
    overrides: Dict[str, Any] = {}
    if ns.duration is not None:
        overrides["duration_sec"] = int(ns.duration)
    if ns.rps is not None:
        overrides["rps"] = int(ns.rps)

    if ns.mix:
        # k=v 목록 → dict
        overrides["mix"] = _parse_kv_map(ns.mix)

    if ns.error_rate:
        overrides["error_rate"] = _parse_error_rate(ns.error_rate)

    overrides["dry_run"] = bool(ns.dry_run)
    overrides["replicate_error_to_topic"] = not bool(ns.no_error_repl)
    overrides["log_interval_sec"] = int(ns.log_interval)

    return profile_path, routes_path, overrides


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """
    최종적으로 argparse.Namespace를 반환한다.
    - main.py 등에서 바로 사용 가능

    Args:
        argv: 테스트/주입용 인자 리스트. None이면 sys.argv 사용.

    Returns:
        argparse.Namespace: 인자 파싱 결과
    """
    parser = build_parser()
    return parser.parse_args(argv)
