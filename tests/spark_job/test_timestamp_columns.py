# -----------------------------------------------------------------------------
# 목적  : 타임스탬프 리팩터링 후 컬럼명 정합성 검증
#         - FACT_EVENT_COLUMNS 컬럼 목록
#         - 소스 코드 내 구 컬럼명 잔존 여부
# 실행  : uv run pytest tests/spark_job/test_timestamp_columns.py -v
# -----------------------------------------------------------------------------

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).parent.parent.parent


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


@pytest.fixture(scope="module")
def fact_event_col_set():
    return set(load_fact_event_columns())


@pytest.mark.parametrize("expected_col", sorted(EXPECTED_FACT_TIMESTAMP_COLUMNS))
def test_fact_event_columns_포함(fact_event_col_set, expected_col):
    assert expected_col in fact_event_col_set


@pytest.mark.parametrize("banned_col", sorted(BANNED_FACT_TIMESTAMP_COLUMNS))
def test_fact_event_columns_미포함(fact_event_col_set, banned_col):
    assert banned_col not in fact_event_col_set


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


def _format_hits(hits: dict[str, list[tuple[int, str]]]) -> str:
    lines = ["구 컬럼명 발견:"]
    for file, entries in hits.items():
        lines.append(f"  {file}")
        for lineno, content in entries:
            lines.append(f"    L{lineno}: {content}")
    return "\n".join(lines)


def test_no_banned_column_names():
    hits = scan_banned_patterns()
    assert not hits, _format_hits(hits)


# ---------------------------------------------------------------------------
# 3) ClickHouse DDL 컬럼명 검증
# ---------------------------------------------------------------------------

DDL_EXPECTED_COLUMNS = [
    "event_timestamp",
    "kafka_received_at",
    "spark_received_at",
    "spark_processed_at",
    "clickhouse_stored_at",
]

DDL_BANNED_COLUMNS = [
    "event_ts", "kafka_ingest_ts", "kafka_ts",
    "spark_ts", "processed_ts", "stored_ts",
    "producer_ts", "created_ts",
]


@pytest.fixture(scope="module")
def ddl():
    return (PROJECT_ROOT / "infra" / "clickhouse" / "sql" / "10_fact.sql").read_text()


@pytest.mark.parametrize("column", DDL_EXPECTED_COLUMNS)
def test_clickhouse_ddl_columns_포함(ddl, column):
    assert column in ddl


@pytest.mark.parametrize("column", DDL_BANNED_COLUMNS)
def test_clickhouse_ddl_columns_미포함(ddl, column):
    assert column not in ddl
