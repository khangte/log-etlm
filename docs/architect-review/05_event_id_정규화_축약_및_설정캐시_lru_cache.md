# 우선순위 6·7번 설계 검토 결과

- 요청: lead (docs/architect_review_6_7.md)
- 검토일: 2026-08-21
- 대상: docs/overengineering_audit.md line 20(`_normalize_event_id_value`), line 21(설정 캐시 싱글턴)

---

## 6. `_normalize_event_id_value` 축약

### 결론

축약한다. 함수 자체를 없애고 `_seed_segment()`에 인라인한다(26줄 → 0줄).
`event_id_rule_version`도 함께 제거한다. 다만 `_seed_segment`의 **길이 프리픽스는
남긴다** — 감사 제안과 이 부분만 다르다.

### 입력 타입이 닫혀 있는지 확인

`generate_stable_event_id()`는 `make_http_event()` / `make_domain_event()`가
만든 dict만 받고, seed에 쓰이는 필드는 `event_id_seed_fields` 17개로 고정이다.
호출부 전체(auth/order/payment)를 따라가면 실제 값 타입은 다음으로 닫힌다.

| 타입 | 해당 필드 |
| --- | --- |
| `str` | service, event_name, request_id(uuid4 hex), method, route_template, result, reason_code, api_group, order_id, payment_id |
| `int` | ts_ms, status_code, duration_ms, user_id, timestamp_ms, product_id |
| `float` | amount — `make_domain_event`가 `float(amount)`로 캐스팅. 원본은 `randint(1000, 500000)`이라 항상 정수값 float |
| `None` | 선택 필드 미설정 시 `event.get()` 결과 |

`bytes`, `list`, `tuple`, `dict`, `bool`은 어느 경로로도 들어오지 않는다.
`extra`로 주입되는 값도 `timestamp_ms`(int), `product_id`(int), `amount`(int)뿐이다.
문자열은 전부 ASCII(서비스명, 대문자 reason code, hex id, URL 경로)라
`unicodedata.normalize("NFC", ...)`는 모든 실제 입력에 대해 항등이다.

### 축약안

```python
def _seed_segment(self, name: str, value: Any) -> str:
    text = "" if value is None else str(value)
    return f"{name}:{len(text)}:{text}"
```

`_normalize_event_id_value`는 삭제하고 `import unicodedata`도 함께 제거한다.

float 분기(`is_integer()` → `str(int(v))`, 아니면 `format(v, ".15g")`)도 버린다.
`.15g` 포맷은 repr 불안정성을 막으려는 방어인데, CPython 3.1 이후 `str(float)`은
최단 왕복 repr로 결정적이고 런타임도 3.11 하나뿐이다. `500000.0`이 `"500000"`
대신 `"500000.0"`이 되지만 해시 입력으로서 차이가 없다.

### 길이 프리픽스는 남긴다

감사는 `_seed_segment`의 `name:len:value`에서 길이도 함께 정리하자고 했으나
유지를 권한다. 이 프리픽스가 막는 것은 값 안에 구분자(`|`, `:`)가 섞였을 때
서로 다른 필드 조합이 같은 seed 문자열로 붕괴하는 경우다. 이때 생기는 결과는
서로 다른 두 이벤트가 같은 `event_id`를 갖는 것이고, `ReplacingMergeTree(event_id)`가
둘 중 하나를 조용히 지워 버린다. **경보 없이 데이터가 사라지는 실패**다.
비용은 `len()` 호출 하나이므로 방어를 유지하는 편이 합리적이다.
`event_id_rule_version` 제거와 달리 이건 "쓰이지 않는 유연성"이 아니라 안전장치다.

### `event_id_rule_version` 제거

제거한다. 값은 `"v1"` 하나뿐이고, 저장소 전체에서 이 필드를 읽는 코드는
`base.py` 자기 자신 말고 없다(Spark·ClickHouse·Grafana 모두 `event_id`를 불투명
문자열로만 다룬다). `_v` 세그먼트와 id 프리픽스에서 모두 뺀다.

- 결과 형식: `evt_v1_<32hex>` → `evt_<32hex>`
- 기존 적재분은 `evt_v1_` 형태로 남아 테이블에 두 형식이 섞인다. 불투명 값이라
  기능 영향은 없고 표시상의 문제만 있다. 감수한다.

### 호환성(재현성) 판단

**멱등성 영향 없음.**

`event_id`는 시뮬레이터가 생성 시점에 한 번 계산해 페이로드에 담고, Spark는
`normalize_event.py:31-45`에서 `json.event_id`를 **읽기만** 한다
(누락 시 `topic-partition-offset` 폴백). 어디에서도 저장된 필드로부터 seed를
재계산하지 않는다. 따라서

- 규칙 변경은 **앞으로 생성될 이벤트에만** 영향을 준다.
- Kafka 재전송/재처리 시에도 페이로드에 든 기존 id가 그대로 쓰이므로
  `ReplacingMergeTree(event_id)` 중복 제거는 종전대로 동작한다.
- 규칙이 결정적이기만 하면(같은 필드 → 같은 id) 요구되는 성질은 모두 충족된다.

깨지는 것은 "이미 적재된 행의 필드로부터 그 행의 event_id를 다시 계산해 맞춰보는"
작업뿐인데, 그런 코드는 저장소에 없다.

### 함께 권하는 것

규칙을 새로 확정하는 시점이므로, 고정 입력 dict 하나에 대한 `event_id` 문자열을
그대로 박아 두는 회귀 테스트를 추가한다(약 10줄). 앞으로 seed 규칙이 실수로
바뀌면 그 자리에서 잡힌다. 지금은 이 규칙을 지키는 장치가 하나도 없다.

### 커밋 분리

`refactor(simulator)` 한 건 — `base.py` 단일 파일.
회귀 테스트를 추가한다면 `test`로 분리.

---

## 7. 설정 캐시 싱글턴 → `lru_cache`

### 결론

전환한다. `@lru_cache(maxsize=1)`로 각 6줄 → 1줄.

### 대상 재집계

DLQ 삭제 후 남은 것은 **6개 파일 8개 지점**이다(요청 문서의 "6개 파일" 기준으로는
파일 수가 맞고, 지점 수는 파일당 2개인 곳이 있어 8개다).

동일 패턴 7개 — 그대로 전환:

1. `spark_job/stream_settings.py` `get_stream_ingest_settings()`
2. `spark_job/fact/settings.py` `get_fact_stream_settings()`
3. `spark_job/clickhouse/settings.py` `get_clickhouse_settings()`
4. `spark_job/clickhouse/settings.py` `get_batch_timing_log_settings()`
5. `log_simulator/simulator/settings.py` `get_simulator_settings()`
6. `log_simulator/publisher/settings.py` `get_publisher_settings()`
7. `log_simulator/producer/settings.py` `get_producer_settings()`

예외 1개 — `log_simulator/simulator/settings.py` `get_queue_config(settings=None)`.
선택 인자를 받아 **인자가 없을 때만** 캐시하고, 있으면 매번 새로 만든다.

```python
@lru_cache(maxsize=1)
def get_simulator_settings() -> SimulatorSettings:
    """캐시된 시뮬레이터 설정을 반환한다."""
    return load_simulator_settings()
```

모듈 전역 `_*_cache` 선언과 `global` 문은 함께 삭제한다.

### `get_queue_config` 처리

선택 인자를 없애고 무인자 함수로 만든다.

```python
@lru_cache(maxsize=1)
def get_queue_config() -> QueueThrottleConfig:
    return build_queue_config(get_simulator_settings())
```

호출부는 세 곳이고 모두 안전하다.

- `task_builder.py:22,63` — `get_queue_config(settings)`에 넘기는 `settings`가
  이미 `get_simulator_settings()`가 돌려준 바로 그 캐시 객체다. 무인자 호출로 바꿔도
  같은 값이 나온다.
- `stream_pipeline.py:44` — `queue_config or get_queue_config(resolved_settings)`도
  마찬가지 경로다. `get_queue_config()`로 바꾼다.

임의의 `SimulatorSettings`로 큐 정책을 만들어야 하면 이미 공개된
`build_queue_config(settings)`를 직접 쓰면 된다. 즉 기능이 사라지지 않는다.

`SimulatorSettings`가 `frozen=True`라 `lru_cache`의 인자로 넘기는 것도 문법상
가능하지만, 그렇게 하면 settings 객체마다 항목이 쌓여 캐시 크기가 예측 불가해진다.
무인자 + `maxsize=1`이 현재 사용 실태에 맞는다.

### 테스트에서 캐시 리셋이 필요한가

**필요 없다.** 저장소의 테스트는 캐시 게터를 호출하지 않는다.

- `tests/spark_job/*`는 `load_clickhouse_settings(env)`, `load_fact_stream_settings(env)`처럼
  env 매핑을 인자로 받는 **로더**를 직접 부른다. 캐시를 타지 않는다.
- `tests/simulator/test_api_simulator.py`는 `engine` 객체를 통째로 `patch`한다.

앞으로 환경변수를 바꿔가며 게터를 재호출하는 테스트가 필요해지면
`get_x.cache_clear()`를 쓰면 된다. 오히려 지금의 전역 변수 패턴보다 리셋 수단이
명확해진다(모듈 내부 이름을 직접 건드리지 않아도 된다).

### 동작 차이

- 예외: `lru_cache`는 예외를 캐시하지 않고 다시 던진다. 현재 패턴도 캐시가
  `None`으로 남아 다음 호출에서 재시도하므로 동일하다.
- 스레드: 현재 `if None` 패턴은 경합 시 로드를 두 번 할 수 있다(결과는 같아 무해).
  `lru_cache`는 내부 락이 있어 이 부분은 오히려 개선된다.
- 프로세스 경계: 둘 다 프로세스 단위 캐시라 Spark 익스큐터 동작에 차이가 없다.

런타임은 시뮬레이터 `python:3.11-slim`, Spark `spark:4.0.1-python3`으로 둘 다
`functools.lru_cache` 사용에 문제가 없다.

### 손대지 않는 것

`log_simulator/publisher/dlq.py`의 `get_dlq_publisher()`와 producer의 `get_client()`도
같은 싱글턴 모양이지만, 이들은 설정값이 아니라 **연결·리소스를 쥔 객체**를 만든다.
`lru_cache`로 감싸면 생명주기와 해제 시점이 흐려지므로 이번 전환 대상에서 뺀다.
(`get_dlq_publisher`는 시뮬레이터 DLQ 경로 존폐 판단과도 묶여 있다 —
docs/architect-review/04 참조.)

### 커밋 분리

`refactor(spark)` (spark_job 3파일) / `refactor(simulator)` (log_simulator 3파일)
두 건으로 나눈다. 두 디렉터리는 서로 의존하지 않아 각각 독립적으로 동작한다.
