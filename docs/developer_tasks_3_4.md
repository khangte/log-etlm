# 우선순위 1·2번 구현 지시 (architect 설계 승인 완료)

설계 근거: docs/architect-review/02_테스트_harness_pytest_전환_및_spark_env_계층_삭제.md 참조.

## 1. 테스트 harness → pytest 전환

대상:
- tests/spark_job/test_clickhouse_settings.py
- tests/spark_job/test_fact_settings.py
- tests/spark_job/test_timestamp_columns.py

지시:
- 저장소 루트에 빈 conftest.py 추가 (tests/__init__.py 이미 있어 모듈명 충돌 없음). 추가 후 기존 tests/simulator/test_api_simulator.py 수집 정상 확인.
- 순서: (1) conftest.py (2) clickhouse (그룹1, 패턴 검증) (3) fact (그룹3, 순수함수) (4) timestamp (최대 분량).
- 매핑: check(label, actual, expected) → assert. results 리스트 + all() 그룹 검증 → @pytest.mark.parametrize (첫 실패에서 멈추지 않고 전 케이스 결과를 한 번에 보는 기존 harness 장점 보존 목적 — 단순 assert 나열 금지). 한 시나리오를 이루는 복수 단언(keys+watermark 등)은 한 테스트 함수에 묶기.
- PASS/FAIL 출력, print, __main__ 집계 블록 삭제. sys.path.insert 삭제(conftest.py로 대체).
- timestamp 파일의 _extract_list_from_ast / scan_banned_patterns는 검증 로직이므로 유지 — 출력·집계 계층만 제거.
- 구 컬럼명 스캔 실패 시 진단 메시지(파일/줄번호/내용) 보존 필수: `assert not hits, _format_hits(hits)` 형태로 메시지 인자에 반드시 남길 것.
- "포함" 체크(`"url" in opts` 등)는 동등비교(`== True`)로 바꾸지 말고 `assert "url" in opts` 그대로.
- 각 파일 헤더의 "실행: uv run python ..." 주석을 pytest 실행법으로 갱신.
- 완료 기준: 기존 check() 호출 46건(5/15/26) → 전환 후 pytest 수집 테스트 44개(시나리오 2개가 단언 2개씩 품음). 44개 아니면 매핑 누락이니 재확인.
- requirements에 pytest 없는 문제는 별건 — 이번 지시 범위 아님, 발견 시 별도 보고만.
- test_timestamp_columns.py 자체 존치 여부도 별건, 손대지 말 것.

## 2. Spark env 자동전환 계층 삭제

대상 전부 삭제 (참고용 보존 없음):
- scripts/autoswitch_spark_env.sh
- scripts/apply_spark_env.sh
- scripts/current_spark_env.sh
- config/env/*.env (config/ 디렉터리가 비면 디렉터리도 함께 제거)
- docker-compose.yml의 SPARK_ENV_PROFILE 라인, config/env 볼륨 마운트 라인
- .gitignore의 관련 라인 (142-143 부근)
- README.md의 크론 블록 및 관련 언급
- ARCHITECTURE.md의 관련 언급

**주의**: docker-compose.yml의 다른 env 값(SPARK_STREAM_SHUFFLE_PARTITIONS 등 숫자)은 절대 건드리지 말 것 — 이미 단일 프로파일로 확정된 값이며 mid 프로파일 숫자로 덮어쓰는 게 아니다. SPARK_ENV_PROFILE 라인과 config/env 마운트 라인만 제거.

CLAUDE.md는 이 작업에서 건드리지 말 것 — 별도 미커밋 이슈(architect 판정 01)와 무관하게 여기 스코프 아님.

## 커밋 분리 (참고용, 실제 커밋은 lead가 함)
- chore: 스크립트 3개 + config/env/ + docker-compose.yml + .gitignore
- docs: README.md + ARCHITECTURE.md
- (테스트 전환은 별도 refactor/test 커밋)

## 공통
- 각 항목 완료 후 관련 테스트 돌려서 통과 확인.
- 완료되면 say lead로 보고 (1번/2번 각각 완료 시점 보고해도 되고, 둘 다 끝나면 한 번에 보고해도 됨).
