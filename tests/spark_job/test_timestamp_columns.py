# -----------------------------------------------------------------------------
# 목적  : 타임스탬프 리팩터링 후 컬럼명 정합성 검증
#         - FACT_EVENT_COLUMNS / FACT_EVENT_DLQ_COLUMNS 컬럼 목록
#         - 소스 코드 내 구 컬럼명 잔존 여부
# 실행  : uv run python tests/spark_job/test_timestamp_columns.py
# -----------------------------------------------------------------------------

from __future__ import annotations

import ast
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

PASS = "✅"
FAIL = "❌"

PROJECT_ROOT = Path(__file__).parent.parent.parent


def check(label: str, actual, expected) -> bool:
    ok = actual == expected
    print(f"  {'✅' if ok else '❌'} {label}")
    if not ok:
        print(f"       expected: {expected!r}")
        print(f"       actual  : {actual!r}")
    return ok


# ---------------------------------------------------------------------------
# 1) FACT_EVENT_COLUMNS 컬럼 목록 검증
# ---------------------------------------------------------------------------

EXPECTED_FACT_TIMESTAMP_COLUMNS = {
    "event_timestamp",
    "kafka_received_at",
    "spark_received_at",
    "spark_processed_at",
}

BANNED_FACT_TIMESTAMP_COLUMNS = {
    "event_ts",
    "kafka_ingest_ts",
    "kafka_ts",
    "spark_ts",
    "processed_ts",
    "stored_ts",
    "producer_ts",
    "created_ts",
}


def _extract_list_from_ast(path: Path, var_name: str) -> list[str]:
    """AST에서 list[str] 타입 어노테이션 변수 값을 파싱해 반환한다."""
    tree = ast.parse(path.read_text())
    for node in ast.walk(tree):
        # list[str] 어노테이션이 있는 경우 (AnnAssign)
        if isinstance(node, ast.AnnAssign):
            if isinstance(node.target, ast.Name) and node.target.id == var_name:
                if isinstance(node.value, ast.List):
                    return [elt.value for elt in node.value.elts if isinstance(elt, ast.Constant)]
        # 어노테이션 없는 경우 (Assign)
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == var_name:
                    if isinstance(node.value, ast.List):
                        return [elt.value for elt in node.value.elts if isinstance(elt, ast.Constant)]
    raise RuntimeError(f"{var_name}를 찾을 수 없습니다.")


def load_fact_event_columns() -> list[str]:
    """FACT_EVENT_COLUMNS 값을 AST로 파싱해 반환한다."""
    return _extract_list_from_ast(PROJECT_ROOT / "spark_job" / "fact" / "schema.py", "FACT_EVENT_COLUMNS")


def load_fact_event_dlq_columns() -> list[str]:
    """FACT_EVENT_DLQ_COLUMNS 값을 AST로 파싱해 반환한다."""
    return _extract_list_from_ast(PROJECT_ROOT / "spark_job" / "dlq" / "schema.py", "FACT_EVENT_DLQ_COLUMNS")


def test_fact_event_columns() -> bool:
    print("=== FACT_EVENT_COLUMNS 타임스탬프 컬럼 검증 ===")
    cols = load_fact_event_columns()
    col_set = set(cols)

    results = []
    for expected_col in sorted(EXPECTED_FACT_TIMESTAMP_COLUMNS):
        results.append(check(f"{expected_col} 포함", expected_col in col_set, True))

    for banned_col in sorted(BANNED_FACT_TIMESTAMP_COLUMNS):
        results.append(check(f"{banned_col} 미포함", banned_col not in col_set, True))

    return all(results)


def test_fact_event_dlq_columns() -> bool:
    print("\n=== FACT_EVENT_DLQ_COLUMNS 타임스탬프 컬럼 검증 ===")
    cols = load_fact_event_dlq_columns()
    col_set = set(cols)

    expected = {"kafka_received_at", "spark_processed_at", "event_timestamp"}
    banned = {"kafka_ingest_ts", "kafka_ts", "processed_ts", "producer_ts", "created_ts"}

    results = []
    for c in sorted(expected):
        results.append(check(f"{c} 포함", c in col_set, True))
    for c in sorted(banned):
        results.append(check(f"{c} 미포함", c not in col_set, True))

    return all(results)


# ---------------------------------------------------------------------------
# 2) 소스 코드 내 구 컬럼명 잔존 여부 (정적 grep)
# ---------------------------------------------------------------------------

BANNED_PATTERNS = [
    r"\bevent_ts\b",
    r"\bkafka_ingest_ts\b",
    r"\bkafka_ts\b",
    r"\bspark_ts\b",
    r"\bprocessed_ts\b",
    r"\bstored_ts\b",
    r"\bproducer_ts\b",
    r"\bcreated_ts\b",
]

SCAN_DIRS = ["spark_job", "infra/clickhouse/sql"]
SCAN_EXTENSIONS = {".py", ".sql"}
EXCLUDE_PATHS = {"__pycache__"}


def scan_banned_patterns() -> dict[str, list[tuple[int, str]]]:
    """소스 코드에서 구 컬럼명 패턴을 탐색한다."""
    combined = re.compile("|".join(BANNED_PATTERNS))
    hits: dict[str, list[tuple[int, str]]] = {}

    for scan_dir in SCAN_DIRS:
        base = PROJECT_ROOT / scan_dir
        for path in base.rglob("*"):
            if not path.is_file():
                continue
            if path.suffix not in SCAN_EXTENSIONS:
                continue
            if any(ex in path.parts for ex in EXCLUDE_PATHS):
                continue
            # 테스트 파일 자체 제외
            if path.name.startswith("test_"):
                continue
            lines = path.read_text().splitlines()
            for i, line in enumerate(lines):
                if combined.search(line):
                    rel = str(path.relative_to(PROJECT_ROOT))
                    hits.setdefault(rel, []).append((i + 1, line.strip()))
    return hits


def test_no_banned_column_names() -> bool:
    print("\n=== 소스 코드 내 구 컬럼명 잔존 검사 ===")
    hits = scan_banned_patterns()
    if not hits:
        print(f"  {PASS} 구 컬럼명 없음")
        return True

    print(f"  {FAIL} 구 컬럼명 발견:")
    for file, lines in hits.items():
        print(f"    {file}")
        for lineno, content in lines:
            print(f"      L{lineno}: {content}")
    return False


# ---------------------------------------------------------------------------
# 3) ClickHouse DDL 컬럼명 검증
# ---------------------------------------------------------------------------

def test_clickhouse_ddl_columns() -> bool:
    print("\n=== ClickHouse DDL 타임스탬프 컬럼 검증 ===")
    ddl = (PROJECT_ROOT / "infra" / "clickhouse" / "sql" / "10_fact.sql").read_text()

    expected = [
        "event_timestamp",
        "kafka_received_at",
        "spark_received_at",
        "spark_processed_at",
        "clickhouse_stored_at",
    ]
    banned = [
        "event_ts", "kafka_ingest_ts", "kafka_ts",
        "spark_ts", "processed_ts", "stored_ts",
        "producer_ts", "created_ts",
    ]

    results = []
    for c in expected:
        results.append(check(f"{c} 포함", c in ddl, True))
    for c in banned:
        results.append(check(f"{c} 미포함", c not in ddl, True))

    return all(results)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    results = [
        test_fact_event_columns(),
        test_fact_event_dlq_columns(),
        test_no_banned_column_names(),
        test_clickhouse_ddl_columns(),
    ]
    print()
    if all(results):
        print(f"{PASS} 전체 통과")
    else:
        print(f"{FAIL} 실패 케이스 있음")
        raise SystemExit(1)
