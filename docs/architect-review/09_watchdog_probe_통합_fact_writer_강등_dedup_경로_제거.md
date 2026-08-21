# 설계 검토 — 우선순위 14~18

대상: `docs/overengineering_audit.md` 우선순위 14·15·16·17·18번
판정: 5건 모두 승인. 15·16번은 감사 문서의 경로가 이미 낡아 범위를 좁혔고,
17번은 문서화된 롤백 수단을 지우는 건이라 조건부로 승인한다.

기준 커밋: `e7826c5` (9~13번 반영 후 상태).

**항목 간 의존**: 15·16·17번이 서로 같은 파일을 건드린다. 독립 커밋이되 **16 → 17 → 15 순서**로
진행해야 한다(근거는 각 항목과 마지막 커밋 절에 적었다). 14·18번은 완전히 독립이다.

---

## 14. `spark_rest_probe`/`grafana_health_probe` 통합

### 실측

두 함수는 URL과 메시지 접두사만 다르고 나머지가 문자 단위로 같다. 반환형(`None`),
루프 구조(무한 `while True`), 타임아웃(5초), 예외 튜플 5종, 실패 시 알림 3분기,
`asyncio.sleep(HEALTH_INTERVAL_SEC)` 모두 동일하다. 접두사만 `spark REST` / `grafana health`다.
합쳐도 알림 문자열이 바뀌지 않는다.

### 설계

```python
HTTP_PROBES: tuple[tuple[str, str], ...] = (
    ("spark REST", "http://localhost:4040/api/v1/applications"),
    ("grafana health", "http://localhost:3000/api/health"),
)


async def http_probe(name: str, url: str) -> None:
    """HTTP 엔드포인트를 주기적으로 호출해 응답성을 확인한다."""
    while True:
        try:
            with request.urlopen(url, timeout=5) as resp:
                if resp.status != 200:
                    await send_alert(f"{name} returned HTTP {resp.status}")
        except OSError as exc:
            await send_alert(f"{name} unreachable: {exc}")
        except Exception as exc:
            await send_alert(f"{name} probe error: {exc}")
        await asyncio.sleep(HEALTH_INTERVAL_SEC)
```

`main.py`는 `LOG_PATTERNS`를 순회하는 기존 방식과 같은 모양으로 바꾼다.

```python
from watchdog.service_probes import HTTP_PROBES, http_probe
...
for name, url in HTTP_PROBES:
    tasks.append(asyncio.create_task(http_probe(name, url)))
```

`main.py`의 24-25줄(`spark_rest_probe()`/`grafana_health_probe()` 생성) 두 줄이 사라지고
17줄 import가 위와 같이 바뀐다.

### 예외 튜플 축약

기존 5종은 전부 `OSError`의 하위 클래스다.

```
OSError
├── urllib.error.URLError (→ HTTPError 포함)
├── TimeoutError            (Python 3.10부터 OSError 하위, socket.timeout의 별칭)
└── ConnectionError
    └── ConnectionResetError
        └── http.client.RemoteDisconnected
```

따라서 `except OSError`로 축약해도 잡는 범위가 정확히 같다.
`import http.client`와 `import urllib.error`도 함께 제거된다.
뒤따르는 `except Exception`은 남긴다 — `urlopen`이 `ValueError`(잘못된 URL) 등
`OSError`가 아닌 예외를 낼 수 있고, probe 루프가 죽으면 감시가 조용히 멎는다.

51줄 → 약 22줄.

---

## 15. `ClickHouseFactWriter` 모듈 함수 강등

### 감사 전제 정정

`spark_job/dlq/`는 존재하지 않는다(DLQ 파이프라인 삭제 시 제거). 이 항목의 대상은
`ClickHouseFactWriter` **하나뿐**이며, `ClickHouseDlqWriter` 부분은 무효다.

### 실측

메서드 1개(`write_event_log_stream`), 호출자 1곳(`stream_ingest.py:25,35`), 상태는
`self._settings` 하나뿐이고 그 값은 `get_fact_stream_settings()`로 어디서든 얻을 수 있다.
클래스가 제공하는 것은 인스턴스 하나를 만들어 바로 메서드 하나를 부르는 왕복뿐이다.

### 설계

5·9번에서 확립한 패턴대로 주입 파라미터 없이 내부에서 설정을 읽는다.

```python
def write_event_log_stream(df: DataFrame):
    """팩트 이벤트 스트림을 적재한다."""
    settings = get_fact_stream_settings()
    ...
```

- `spark_job/fact/writers/fact_writer.py`
  - `class ClickHouseFactWriter` / `__init__` 제거, 메서드를 모듈 함수로 승격
  - `FactStreamSettings` import 제거(`__init__` 주석용으로만 쓰였다), `get_fact_stream_settings`는 유지
- `spark_job/stream_ingest.py`
  - 14줄 import → `from .fact.writers.fact_writer import write_event_log_stream`
  - 25줄 `fact_writer = ClickHouseFactWriter(fact_settings)` 삭제
  - 35줄 → `write_event_log_stream(event_df)`

`stream_ingest`가 들고 있는 `fact_settings`는 체크포인트 경로와 `KafkaStreamBuilder`에 계속 필요하고,
`get_fact_stream_settings()`가 `lru_cache`라 함수 안에서 읽는 값과 **동일한 객체**다. 설정이 갈라질 여지가 없다.

---

## 16. `SPARK_STREAM_SHUFFLE_PARTITIONS` 제거

### 감사 전제 정정

`config/env/*.env`는 이미 삭제됐다(우선순위 2번). 프로파일별 4/8/10 값은 저장소에 남아 있지 않다.
현재 남은 것은 `docker-compose.yml:235`의 `SPARK_STREAM_SHUFFLE_PARTITIONS=${...:-}` 한 줄이다.

### 이 분기가 죽어 있는 이유는 두 겹이다

1. 게이트: `spark.py:125-126`이 `SPARK_FACT_DEDUP_KEYS`가 비어 있지 않을 때만 값을 읽는다.
2. 그 `SPARK_FACT_DEDUP_KEYS`는 `docker-compose.yml:238`에 `SPARK_FACT_DEDUP_KEYS=`로 **하드코딩**돼 있다.
   `${VAR:-}` 형태가 아니라서 `.env`로 덮을 수조차 없다.

즉 compose를 직접 고치지 않는 한 어떤 경로로도 도달하지 않는다. 게다가 compose 235줄의 기본값도
비어 있어서, 설령 게이트가 열려도 `get_env_int`가 `None`을 돌려준다.

### 설계

- `spark_job/spark.py`
  - 125-129줄(`dedup_keys` 읽기 + 분기) 삭제
  - 138줄 `shuffle_partitions=shuffle_partitions` 인자 삭제
  - `_build_spark_session`의 `shuffle_partitions` 파라미터(41)와 적용부(63-64) 삭제 —
    **호출자가 `build_streaming_spark` 하나뿐**임을 확인했다
  - `get_env_int`가 이 파일에서 더 쓰이지 않으면 import 정리
- `docker-compose.yml:235` 삭제
- `README.md` 168줄의 `SPARK_STREAM_SHUFFLE_PARTITIONS` 언급 제거. 128-129줄은 파티션 흐름 설명(`SHUFFLE_PARTITIONS=3`, `dropDuplicates 셔플`)이라 17번 문서 커밋에서 함께 정리한다

`spark.sql.shuffle.partitions`는 지정하지 않으면 Spark 기본값(200)이 쓰이지만, dedup을 없앤 뒤
이 파이프라인에는 셔플 자체가 없다(`coalesce`는 셔플이 아니다). 값이 무의미하므로 손실이 없다.

### 17번과의 순서

이 항목이 `SPARK_FACT_DEDUP_KEYS`를 읽는 마지막 코드를 지운다. 16번을 먼저 하면 17번은
`spark.py`를 건드리지 않아도 된다. 반대로 하면 17번이 `spark.py`의 죽은 참조를 떠안는다.
**16번 먼저.**

---

## 17. dedup 3중 경로 중 Spark 쪽 2개 제거 — 조건부 승인

### 실측

- watermark 경로(`fact_writer.py:22-38`): `batch_dedup_keys and dedup_watermark` 둘 다 필요
- 배치 경로(`writer_base.py:81-83`): `deduplicate_keys`만 있으면 동작
- 두 경로 모두 `SPARK_FACT_DEDUP_KEYS`에서 출발하는데, 그 값은 compose에 빈 값으로 하드코딩
  (16번 참조). `SPARK_FACT_DEDUP_WATERMARK`는 `${...:-}`로 덮을 수 있지만 키가 없으면 무의미하다.
- 실제 중복 제거는 ClickHouse `ReplacingMergeTree(event_id)`가 담당한다.

### 왜 조건부인가

이 둘은 단순히 잊힌 코드가 아니라 **계측해 보고 껐던** 설정이다.
`docs/pipeline_issue_log.md`에 watermark는 상태 저장소 부담으로, `dropDuplicates`는 셔플로 인한
JDBC 커넥션 급증과 executor 메모리 압박으로 각각 제거한 경위가 남아 있고,
`docs/stability_config_rationale.md:135`는 이 값을 "필요 시 `event_id` 재활성화"라는
**롤백 수단**으로 표에 올려두고 있다.

그럼에도 삭제를 승인하는 근거:

1. 현재 형태로는 롤백 수단이 아니다. compose를 편집해야만 켤 수 있으므로, 코드를 고치는 것과
   손이 가는 정도가 같다.
2. 두 경로 다 시도했다가 계측 근거로 되돌린 설정이다. 남겨 둔 분기는 "다시 켤 수 있다"가 아니라
   "다시 켜면 안 되는 걸 알고 있다"에 가깝다.
3. 되살릴 때 필요한 코드는 `out_df.dropDuplicates(["event_id"])` 한 줄이다.

**조건**: `docs/stability_config_rationale.md`의 롤백 표에서 해당 행을 지우지 말고
"Spark dedup 경로는 제거됨 — 재도입 시 `writer_base`에 `dropDuplicates` 재삽입"으로 고쳐 쓸 것.
왜 껐는지가 기록에서 사라지면 나중에 같은 실험을 반복하게 된다.
`docs/pipeline_issue_log.md`는 이력 문서이므로 손대지 않는다.

### 삭제 범위

| 파일 | 내용 |
| --- | --- |
| `spark_job/fact/writers/fact_writer.py` | 19-38줄 dedup 블록 전체(예외 검사 2개 포함), 46줄 `deduplicate_keys=` 인자 |
| `spark_job/clickhouse/writer_base.py` | 26줄 파라미터, 81-83줄 `dropDuplicates`, 102-103줄 삼항을 `current_partitions=current_parts`로 |
| `spark_job/fact/settings.py` | 23-24줄 필드 2개, 27-32줄 `_parse_dedup_keys`, 46-47줄 로더 2줄, import에서 `get_env_optional_str` 제거 |
| `docker-compose.yml` | 238-239줄 |
| `tests/spark_job/test_fact_settings.py` | **파일 전체 삭제** |

`get_env_optional_str` 자체는 `stream_settings.py`가 2곳에서 쓰므로 `common/get_env.py`에 남긴다
(그 함수의 중복 문제는 우선순위 19번 별건).

`current_parts`는 `_foreach` 진입부에서 `None`으로 초기화되고 pre_coalesce 경로에서만 값이 채워지므로,
삼항을 없애고 그대로 넘겨도 기존 비-dedup 동작과 완전히 같다.

### 예외 검사 2개

`event_timestamp column is required...`와 `missing dedup columns...`는 watermark 분기 안에서만
의미가 있다. 분기가 사라지면 검사할 대상 자체가 없으므로 함께 삭제한다.
`event_timestamp` 컬럼 존재 여부는 스키마(`fact/schema.py`)와 `normalize_event`가 보장한다.

### 테스트 영향 — 파일 하나가 통째로 없어진다

`tests/spark_job/test_fact_settings.py` 57줄은 4개 테스트가 전부 dedup 키/워터마크 파싱만 검증한다
(`_parse_dedup_keys` 파라미터화, watermark 파싱, 둘 조합, 키만 있는 경우). 대상 코드가 사라지면
남는 테스트가 없다. 우선순위 1번에서 방금 pytest로 옮긴 파일이지만, 검증 대상이 없어진 이상
빈 껍데기를 남기는 것보다 파일을 지우는 게 맞다. developer는 이 삭제를 커밋 메시지에 명시할 것.

### 문서 수정 대상

`ARCHITECTURE.md` 66·199-200·204·240-241, `docs/e2e_pipeline_flow.md` 51·516-564·580,
`docs/tech_stack_rationale.md` 90, `docs/stability_config_rationale.md` 56·77·135(위 조건대로 개정).
`docs/pipeline_issue_log.md`는 이력이므로 제외.

---

## 18. `_apply_partitioning` 업스케일 분기 제거

### 감사 전제 정정

"세 프로파일 모두 false 고정"은 `config/env/`가 있던 시절 표현이다. 현재는
`docker-compose.yml:247`의 `${SPARK_CLICKHOUSE_ALLOW_REPARTITION:-false}` 하나이고,
`.env`로 덮을 수는 있다(현재 `.env`에는 없다). 16·17번과 달리 **문법상 도달 가능한 경로**다.

### 그럼에도 제거하는 근거

업스케일이 실행되려면 `write_partitions > current`여야 한다. 현재 값은
`SPARK_CLICKHOUSE_WRITE_PARTITIONS=3`, 그 앞단 `pre_coalesce_partitions=3`, Kafka 입력 파티션 7이다.
`_apply_partitioning`에 들어올 때 `current`는 3 또는 7이라 `n > current`가 성립하지 않는다.
성립시키려면 write_partitions를 4 이상으로 올려야 하는데, 그 값은 JDBC 동시 커넥션 수를
직접 결정하는 값이라 늘리는 방향은 튜닝 이력이 반대한다. 게다가 `repartition`은 셔플이며,
JDBC 쓰기 직전 셔플은 17번에서 제거하는 것과 같은 종류의 비용이다.

### 설계

- `spark_job/clickhouse/sink.py`
  - `_apply_partitioning`의 `allow_repartition` 파라미터 삭제
  - 로그 문자열에서 `allow_repartition=%s`와 대응 인자 삭제
  - 30-32줄을 아래로 축약

    ```python
    if n < current:
        return df.coalesce(n)  # 다운스케일은 셔플 없이 coalesce로 줄여 초소형 파티션 생성을 완화한다.
    return df
    ```

  - 49줄 `allow_repartition=resolved_settings.allow_repartition,` 삭제
- `spark_job/clickhouse/settings.py`
  - 19줄 필드, 55줄 로더 삭제
  - `get_env_bool`은 이 파일에서 이 한 곳에만 쓰이므로 import에서도 제거
- `docker-compose.yml:247` 삭제
- `README.md:176` "리파티션" 줄 삭제

`tests/spark_job/test_clickhouse_settings.py`는 `allow_repartition`을 참조하지 않는다(확인 완료).

---

## 커밋 분리

코드와 그 값을 선언하는 compose 줄은 한 커밋에 넣는다. 문서는 별도.

| # | 커밋 | 대상 |
| --- | --- | --- |
| 1 | `refactor(monitor): spark/grafana probe를 http_probe 하나로 통합` | service_probes.py, main.py |
| 2 | `refactor(spark): SPARK_STREAM_SHUFFLE_PARTITIONS 죽은 분기 제거` | spark.py, docker-compose.yml |
| 3 | `refactor(spark): Spark dedup 경로 제거하고 ReplacingMergeTree에 위임` | fact_writer.py, writer_base.py, fact/settings.py, docker-compose.yml |
| 4 | `test(spark): dedup 설정 삭제로 대상이 사라진 test_fact_settings 제거` | tests/spark_job/test_fact_settings.py |
| 5 | `refactor(spark): ClickHouseFactWriter를 모듈 함수로 강등` | fact_writer.py, stream_ingest.py |
| 6 | `refactor(spark): _apply_partitioning 업스케일 분기 제거` | sink.py, clickhouse/settings.py, docker-compose.yml |
| 7 | `docs: dedup·shuffle·repartition 설정 제거 반영` | README.md, ARCHITECTURE.md, docs/* |

**순서 제약**: 2 → 3 → 5는 반드시 이 순서다.
2번이 `SPARK_FACT_DEDUP_KEYS`를 읽는 마지막 코드를 지우고, 3번이 그 설정을 삭제하며,
5번은 3번이 본문을 비운 뒤의 `fact_writer.py`를 강등한다(3번보다 먼저 하면 같은 블록을 두 번 고치게 된다).
4번은 3번 직후. 1·6번은 독립이라 아무 위치나 가능하다.

## 검증

- 1번: `python3 infra/monitor/main.py` 기동 후 Spark/Grafana를 한 번씩 내려 보고
  `spark REST unreachable:` / `grafana health unreachable:` 문자열이 변경 전과 같이 나오는지 확인.
- 2·3·5번: Spark 재기동 후 드라이버 로그에 `StreamingQueryException` 없이 배치가 도는지,
  `batch_timing.log`의 `stage=transform` 소요가 이전과 같은 수준인지 확인.
  3번은 셔플 제거라 오히려 짧아질 수 있으며, 늘어나면 잘못된 것이다.
- 6번: `[clickhouse sink] 파티션 조정 target=3 current=3` 로그가 그대로 나오는지 확인
  (`allow_repartition=` 항목만 빠진다).
