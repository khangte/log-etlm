# 우선순위 1·2번 설계 검토 결과

- 요청: lead (docs/architect_review_1_2.md)
- 검토일: 2026-08-21
- 대상: docs/overengineering_audit.md line 13(stdlib 테스트 harness), line 17(yagni Spark env 자동전환 계층)

---

## 1. 테스트 harness → pytest 전환

### 결론

전환 승인. 다만 감사 문서의 `~493줄`은 실측과 다르다. 세 파일 합계는 360줄
(clickhouse 64 / fact 97 / timestamp 199)이며, 전환 후 예상은 약 155줄이다.
감사 문서의 수치를 360으로 정정한다.

### 전환 원칙 (assertion 매핑)

| 기존 harness 구성요소 | 전환 후 |
| --- | --- |
| `check(label, actual, expected)` | `assert actual == expected`, label은 테스트 이름 또는 parametrize id로 이동 |
| `results = [check(...), ...]` + `all(results)` | `@pytest.mark.parametrize` — 케이스당 독립 테스트 |
| `PASS` / `FAIL` 상수, 출력 `print` | 삭제 |
| `if __name__ == "__main__":` 집계 블록 | 삭제 |
| `sys.path.insert(...)` | 삭제. 저장소 루트에 빈 `conftest.py` 추가로 대체 |

`results` 리스트를 단순히 여러 줄의 `assert`로 바꾸면 첫 실패에서 중단되어,
"모든 케이스를 돌린 뒤 실패 목록을 한 번에 보여준다"는 기존 harness의 유일한
장점이 사라진다. 그래서 그룹 단위 검증은 반드시 `parametrize`로 옮긴다.
반대로 하나의 시나리오를 이루는 복수 단언(예: keys와 watermark를 함께 확인)은
parametrize로 쪼개지 않고 한 테스트 안에 둔다.

`sys.path` 조작은 저장소 루트에 빈 `conftest.py`를 두면 불필요하다.
pytest가 rootdir을 `sys.path` 앞에 넣어주고, `tests/__init__.py`와
`tests/spark_job/__init__.py`가 이미 있어 모듈명 충돌도 없다.
`spark_job/clickhouse/settings.py`와 `spark_job/fact/settings.py`는
`common.get_env`만 import하므로 pyspark 없이 수집·실행된다.

### 파일별 설계

**tests/spark_job/test_clickhouse_settings.py (64줄 → 약 30줄)**

- 모듈 스코프 fixture `opts`: 고정 env dict로 `load_clickhouse_settings(env)`를
  만들고 `build_jdbc_options("analytics.event_log")` 결과를 반환한다.
- `parametrize("key,expected")` 4건: driver, dbtable, isolationLevel, batchsize.
- `"url" in opts` 1건은 동등 비교가 아니므로 별도 테스트에서 `assert "url" in opts`로
  둔다. `assert ("url" in opts) == True` 형태로 옮기지 않는다.

**tests/spark_job/test_fact_settings.py (97줄 → 약 45줄)**

- `_parse_dedup_keys` 6건 → `parametrize("raw,expected")`.
- watermark 5건 → `parametrize("env,expected")`, 본문은
  `assert load_fact_stream_settings(env).deduplicate_watermark == expected`.
  기존의 지역 함수 `settings()`는 삭제한다.
- 조합 검증 2건은 시나리오 테스트 2개로 남긴다
  (keys+watermark 동시 설정, keys만 설정). 각 테스트 안에 단언 2개.

**tests/spark_job/test_timestamp_columns.py (199줄 → 약 80줄)**

`_extract_list_from_ast`, `load_fact_event_columns`, `scan_banned_patterns`는
harness가 아니라 실제 검증 로직이므로 그대로 유지한다. 바뀌는 것은 그 위에
얹힌 출력·집계 계층뿐이다.

- 컬럼 목록 검증: 모듈 스코프 fixture로 AST 파싱 1회. expected 4건과 banned 8건을
  각각 `parametrize`로 분리.
- 구 컬럼명 스캔: 단일 테스트 `assert not hits, _format_hits(hits)`.
  기존 harness는 실패 시 `파일 / L<n> / 내용`을 출력했는데, pytest는 단언식만
  보여주므로 이 진단 문자열을 메시지 인자로 반드시 넘긴다. **전환 중 가장 놓치기
  쉬운 지점이다.**
- DDL 검증: fixture로 `10_fact.sql` 1회 읽고 expected 5건 / banned 8건 parametrize.

### 순서

전제 단계를 먼저 두고, 작은 파일부터 올라간다. 실패 원인이 섞이지 않게 각 단계마다
테스트를 돌린다.

1. 저장소 루트에 빈 `conftest.py` 추가 → `uv run pytest tests/ -q`로 기존
   `test_api_simulator.py` 수집이 깨지지 않는지 확인.
2. `test_clickhouse_settings.py` 전환 (그룹 1개, 패턴 검증용).
3. `test_fact_settings.py` 전환 (그룹 3개, 순수 함수라 부작용 없음).
4. `test_timestamp_columns.py` 전환 (분량 최대 + 진단 메시지 보존 필요).

### 커버리지 누락 확인 기준

전환 전 `check()` 호출 수는 clickhouse 5, fact 15, timestamp 26으로 합계 46건이다.
전환 후 수집되는 테스트는 시나리오 테스트 2개가 단언 2개씩 품으므로 **44개**여야 한다.
`uv run pytest tests/spark_job -q`의 수집 개수가 44가 아니면 케이스가 누락된 것이다.

그 외 확인 항목:

- 각 파일 헤더 주석의 `실행 : uv run python tests/...` 문구를 `uv run pytest ...`로 갱신한다.
- `_extract_list_from_ast`의 `RuntimeError` 경로는 정당한 실패이므로 skip으로 바꾸지 않는다.
- `scan_banned_patterns`의 `test_` 파일 제외 규칙은 유지한다.
- 어떤 requirements 파일에도 pytest가 없다. 현재는 `uv run pytest`가 임시로 해결해
  주고 있을 뿐이므로, 개발용 의존성 선언을 추가할지는 별건으로 판단이 필요하다.

### 범위 밖으로 남기는 판단

`test_timestamp_columns.py`는 이미 끝난 컬럼명 리팩터링의 일회성 회귀 가드이고,
구 컬럼명 스캔(그룹 2)은 컬럼 목록·DDL 검증(그룹 1·3)과 상당 부분 겹친다.
파일 자체의 존치 여부는 별도 판단 사항으로 남기고, 이번 작업은 전환만 수행한다.

---

## 2. Spark env 자동전환 계층

### 결론

세 스크립트와 `config/env/` 디렉터리를 **모두 삭제한다.** 참고용으로도 남기지 않는다.
docker-compose 기본값이 이미 mid 프로파일과 사실상 같으므로, 값을 옮겨 담을 필요도 없다.

### 근거

1. 세 스크립트 모두 존재하지 않는 경로 `/home/kang/log-etlm`을 하드코딩한다.
   실제 루트는 `/home/kang/projects/log-etlm`이다. 지금 실행하면 즉시 실패한다.
2. `crontab -l`이 비어 있다. README가 안내하는 크론 등록은 실제로 되어 있지 않고,
   그 크론 라인 역시 같은 잘못된 경로를 쓴다.
3. `SPARK_ENV_PROFILE`은 docker-compose가 컨테이너로 넘기기만 할 뿐 코드 어디에서도
   읽지 않는다. 프로파일 라벨이 런타임 동작에 영향을 주지 않는다.
4. 프로파일 간 차이가 가장 큰 `SPARK_STREAM_SHUFFLE_PARTITIONS`(4/8/10)는
   `SPARK_FACT_DEDUP_KEYS`가 비어 있지 않을 때만 적용되는데 그 값은 빈 문자열로
   고정되어 있다. 즉 이 숫자는 이미 아무 데도 반영되지 않는다(감사 line 36).
5. 남는 실질 차이는 `SPARK_MAX_OFFSETS_CAP`(16000/50000/44000),
   `PRE_COALESCE`·`WRITE_PARTITIONS`(1/2/2), `JDBC_BATCHSIZE`(10000/15000/20000)
   세 가지뿐이고, compose 기본값(각각 40000 / 2·2 / 15000)이 mid와 거의 일치한다.
6. 더 중요한 문제로, `apply_spark_env.sh`는 `--force-recreate`로 spark-driver를
   재기동한다. `SPARK_RESET_CHECKPOINT_ON_START=true`인 현재 설정에서는 재기동이
   latest 오프셋부터의 재소비를 의미하므로, 밀린 백로그가 버려진다.
   지연이 커졌을 때 scale-up 하겠다는 이 계층의 목적과 정반대로 동작한다.
   쓰이지 않는 정도가 아니라, 실행되면 해로운 코드다.

### 값 흡수 방침

"흡수"는 mid의 숫자를 compose로 옮겨 적는 것이 아니라, **이미 compose 기본값이
단일 프로파일이라는 사실을 확정하는 것**이다. compose의 현재 값은 4초 트리거
기준으로 실측·튜닝된 운영 값이고(`docs/spark_tuning_log.md`), mid의 CAP 50000은
현재 구성에서 검증된 적이 없다. **compose의 값은 하나도 바꾸지 않는다.**

### 삭제 범위

1. `scripts/autoswitch_spark_env.sh`, `scripts/apply_spark_env.sh`,
   `scripts/current_spark_env.sh` — 182줄
2. `config/env/{low,mid,high}.env` — 45줄. `config/env/` 및 비게 되는 `config/` 제거
3. `docker-compose.yml` — `SPARK_ENV_PROFILE` 환경변수 라인(237),
   `./config/env:/app/config/env:ro` 마운트(255). 이미지 안에서 이 경로를 읽는 코드는 없다
4. `.gitignore` 142-143번 줄 (`!config/env/`, `!config/env/*.env`)
5. `README.md` — "7. (선택) Spark 프로파일 자동 전환 크론 등록" 블록,
   `config/env/{low,mid,high}.env` 안내(171), 유틸 스크립트 목록의 Spark 프로파일 항목(195)
6. `ARCHITECTURE.md` 410번 줄 디렉터리 트리 항목
7. `CLAUDE.md` 환경변수 우선순위에서 `@config/env/` 제거

### 커밋 분리

- `chore`: 스크립트 3개 + `config/env/` + docker-compose + `.gitignore`
- `docs`: README + ARCHITECTURE.md
- `CLAUDE.md` 수정은 이 두 커밋에 섞지 않는다. 해당 파일은 현재
  `ai-setup:team` 자동 주입분이 워킹트리에 미커밋 상태로 남아 있어
  (docs/architect-review/01_claude_md_diff_판정.md), 그 별도 커밋에 함께 넣는다.

### 후속

`SPARK_STREAM_SHUFFLE_PARTITIONS` 제거(감사 line 36)는 `config/env/`가 사라진 뒤
처리하는 편이 쉽다. 별도 항목으로 순서만 뒤에 둔다.
