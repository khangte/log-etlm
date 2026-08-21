# 설계 검토 — 우선순위 9~13

대상: `docs/overengineering_audit.md` 우선순위 9·10·11·12·13번
판정: 5건 모두 승인. 단 10번은 감사가 적은 것보다 축소 폭이 작고, 11·13번은 감사의 전제가
현재 코드와 어긋나 있어 아래에 정정한다.

전제 확인 기준 커밋: `f4e5440` (8번 병합 `a776c2c` 반영 후 상태).

---

## 9. QueueThrottleConfig / build_queue_config / get_queue_config 제거

### 현재 구조

`SimulatorSettings`의 `queue_*` 필드 11개를 접두사만 떼어 그대로 복제한 frozen dataclass다.

| SimulatorSettings | QueueThrottleConfig |
| --- | --- |
| `queue_warn_ratio` | `warn_ratio` |
| `queue_low_watermark_ratio` | `low_watermark_ratio` |
| `queue_low_sleep_scale` | `low_sleep_scale` |
| `queue_throttle_ratio` | `throttle_ratio` |
| `queue_resume_ratio` | `resume_ratio` |
| `queue_throttle_sleep` | `throttle_sleep` |
| `queue_soft_throttle_ratio` | `soft_throttle_ratio` |
| `queue_soft_resume_ratio` | `soft_resume_ratio` |
| `queue_soft_scale_step` | `soft_scale_step` |
| `queue_soft_scale_min` | `soft_scale_min` |
| `queue_soft_scale_max` | `soft_scale_max` |

`build_queue_config()`는 필드 대 필드 대입만 하고, 검증·환산·기본값 보정을 전혀 하지 않는다.
11개 필드 모두 실제로 소비된다(`stream_helpers.apply_queue_backpressure` 8개 +
같은 파일 167-169줄 3개, `stream_pipeline.py:45`의 `soft_scale_max`). 즉 죽은 필드는 없고,
문제는 값이 아니라 **같은 값을 두 번 선언하는 계층 자체**다. 노브를 하나 추가하려면
`SimulatorSettings` 필드 → 로더 → `QueueThrottleConfig` 필드 → `build_queue_config` 대입 →
사용처, 다섯 곳을 고쳐야 한다.

### 설계

`SimulatorSettings`를 그대로 넘기고 참조명을 `settings.queue_*`로 되돌린다.

- `log_simulator/simulator/settings.py`
  - `QueueThrottleConfig` (40-52), `build_queue_config` (198-215), `get_queue_config` (216-218) 삭제 — 약 37줄
- `log_simulator/simulator/stream_helpers.py`
  - `apply_queue_backpressure(logger, *, config: QueueThrottleConfig, ...)`
    → `apply_queue_backpressure(logger, *, settings: SimulatorSettings, ...)`
  - 본문 `config.<name>` → `settings.queue_<name>` 11곳 치환
  - import를 `QueueThrottleConfig` → `SimulatorSettings`로 교체
- `log_simulator/simulator/stream_pipeline.py`
  - `run_simulator_loop`에 남아 있는 주입 파라미터 3개(`log_batch_size`, `settings`, `queue_config`)를 모두 제거한다.
    `queue_config` 제거는 이 항목상 필수고, 나머지 둘도 호출부가 단 한 곳(task_builder)이며 항상
    `get_simulator_settings()`가 준 같은 객체를 넘기고 있어 5번 항목과 같은 근거로 함께 정리한다.

    ```python
    async def run_simulator_loop(
        service: str,
        simulator: Any,
        target_eps: float,
        publish_queue: "asyncio.Queue[list[BatchMessage]]",
    ) -> None:
        settings = get_simulator_settings()
    ```

  - `resolved_settings` / `resolved_queue_config` 지역 변수를 `settings` 하나로 통합,
    `soft_scale_max` 참조(45줄)를 `settings.queue_soft_scale_max`로, 121줄 호출을
    `config=` → `settings=`로 변경
- `log_simulator/simulator/task_builder.py`
  - `queue_config = get_queue_config()` 삭제, `run_simulator_loop(...)` 인자에서
    `log_batch_size`/`settings`/`queue_config` 3개 제거, import에서 `get_queue_config` 제거
  - `settings`는 `loops_per_service`에 계속 필요하므로 유지

### 트레이드오프

`QueueThrottleConfig`를 두는 유일한 명분은 백프레셔 코드가 볼 수 있는 필드를 좁히는 것이었다.
`SimulatorSettings`를 통째로 넘기면 백프레셔 함수가 Kafka·publisher 필드에도 접근 가능해진다.
그러나 이 투영은 검증 로직이 하나도 없는 순수 복사이고, 좁힘의 대가로 손으로 유지하는 필드
목록 두 벌이 생긴다. 필드 접근 범위를 좁히는 일은 타입이 아니라 리뷰로 막는 편이 싸다. 삭제 승인.

### 영향 범위 밖

`SimulatorSettings.queue_size`는 이 투영에 포함된 적이 없고(`assemble_pipeline`이 큐 생성에 직접 사용)
그대로 둔다. 테스트 중 `QueueThrottleConfig`를 참조하는 것은 없다.

---

## 10. watchdog 임계값 점검 함수 통합

### 감사 전제 정정

`_check_dlq_metric`은 **이미 존재하지 않는다**. DLQ 파이프라인 삭제 커밋 `83a33e2`에서 함께
제거됐다. 현재 남은 것은 `_check_ingest_metrics` / `_check_latency_metrics` /
`_check_freshness_metric` 3벌이며, `clickhouse_probe.py:144-146`에서 순차 호출된다.
따라서 "4벌 ~55줄"이 아니라 3벌 약 47줄이 대상이고, 축소 폭도 감사가 적은 것보다 작다.

### 왜 단일 테이블 하나로는 흡수되지 않는가

`_check_latency_metrics`가 쓰는 `(key, value, threshold, stage)` 4열 테이블에 나머지 둘을
그대로 넣을 수 없다. 두 축이 더 필요하다.

1. **비교 방향** — `eps < EPS_MIN`은 하한, 나머지 전부는 상한이다.
2. **메시지 형식** — 넷이 서로 다르다.
   `eps={:.2f} threshold=` / `error_rate_pct={:.3f} threshold=` /
   `p95_ms={:.0f} threshold_ms=` / `ms={:.0f} threshold_ms=`.
   게다가 freshness 메시지에는 `bucket`이 없고, ingest는 `eps_bucket`을, latency는
   `latency_bucket`을 쓴다.

이걸 다 담으면 7열짜리 테이블 + 방향 플래그가 되어 중복 대신 소형 DSL이 생긴다. 그쪽은 반려한다.

### 설계 — 꼬리 부분만 헬퍼로 추출

반복되는 실체는 `breached → breach_ready → should_alert → send_alert` 꼬리 4줄이다. 그것만 뽑고,
메시지 형식은 각 지표가 포맷 문자열로 그대로 들고 있게 한다. 포맷은 위반 시에만 수행되므로
`None` 값에 `:.2f`를 적용할 위험이 없다.

```python
async def _check_threshold(
    key: str,
    now: float,
    value: float | None,
    threshold: float,
    template: str,
    *,
    below: bool = False,
    **fields: object,
) -> None:
    """임계값 위반을 판정하고 grace/cooldown을 거쳐 알림을 보낸다."""
    breached = value is not None and (value < threshold if below else value > threshold)
    if breached and breach_ready(key, now, True) and should_alert(key, now):
        await send_alert(template.format(value=value, threshold=threshold, **fields))
    if not breached:
        breach_ready(key, now, False)
```

호출부는 `_check_ingest_metrics`/`_check_latency_metrics`/`_check_freshness_metric`을 없애고
`_check_metrics(row, now)` 하나로 합친다.

```python
_LATENCY_CHECKS = (
    ("producer_to_kafka_p95",     "producer_to_kafka_p95_ms",     P95_PRODUCER_TO_KAFKA_MS_MAX,     "producer_to_kafka"),
    ("kafka_to_spark_ingest_p95", "kafka_to_spark_ingest_p95_ms", P95_KAFKA_TO_SPARK_INGEST_MS_MAX, "kafka_to_spark_ingest"),
    ("spark_processing_p95",      "spark_processing_p95_ms",      P95_SPARK_PROCESSING_MS_MAX,      "spark_processing"),
    ("spark_to_stored_p95",       "spark_to_stored_p95_ms",       P95_SPARK_TO_STORED_MS_MAX,       "spark_to_stored"),
    ("e2e_p95",                   "e2e_p95_ms",                   P95_E2E_MS_MAX,                   "e2e"),
)


async def _check_metrics(row: dict[str, object], now: float) -> None:
    eps_bucket = row.get("eps_bucket")
    await _check_threshold(
        "eps_low", now, _as_float(row.get("eps")), EPS_MIN,
        "stage=ingest eps={value:.2f} threshold={threshold} bucket={bucket}",
        below=True, bucket=eps_bucket,
    )
    await _check_threshold(
        "error_rate", now, _as_float(row.get("error_rate_pct")), ERROR_RATE_PCT_MAX,
        "stage=ingest error_rate_pct={value:.3f} threshold={threshold} bucket={bucket}",
        bucket=eps_bucket,
    )
    latency_bucket = row.get("latency_bucket")
    for key, column, threshold, stage in _LATENCY_CHECKS:
        await _check_threshold(
            key, now, _as_float(row.get(column)), threshold,
            "stage={stage} p95_ms={value:.0f} threshold_ms={threshold} bucket={bucket}",
            stage=stage, bucket=latency_bucket,
        )
    await _check_threshold(
        "freshness", now, _as_float(row.get("freshness_ms")), FRESHNESS_MS_MAX,
        "stage=freshness ms={value:.0f} threshold_ms={threshold}",
    )
```

144-146줄은 `await _check_metrics(row, now)` 한 줄이 된다.

### 동작 동등성

- 알림 문자열은 문자 단위로 동일하다. 기존 f-string이 임계 상수를 그대로 보간했고
  `template.format(threshold=...)`도 같은 `str()` 결과를 낸다(`EPS_MIN`은 `1.0`).
- `breach_ready` 키 이름 8개 전부 유지 — grace 상태가 초기화되지 않는다.
- 판정 순서(ingest 2건 → latency 5건 → freshness 1건)도 그대로다.

약 47줄 → 약 38줄. 줄 수 이득은 크지 않지만, 지표를 하나 추가할 때 고칠 곳이
"테이블 한 줄 또는 호출 한 번"으로 줄고 grace/cooldown 처리 누락 실수가 구조적으로 막힌다.

---

## 11. `_fetch_kafka_partition_count` 제거

### 감사가 짚지 못한 결정적 사실

현재 설정에서 이 경로는 **결과적으로 아무 옵션도 바꾸지 않는다.**

- `SPARK_KAFKA_MIN_PARTITIONS`는 compose에 선언돼 있지 않다 → `settings.kafka_min_partitions`는 `None`
- `SPARK_KAFKA_MIN_PARTITIONS_MULTIPLIER`는 `1` (docker-compose.yml:229)
- 토픽 파티션 합계는 `logs.auth` 3 + `logs.order` 2 + `logs.payment` 1 + `logs.error` 1 = 7
- 따라서 `minPartitions = max(1, int(7 * 1)) = 7`

`minPartitions`는 Kafka 파티션 수보다 클 때만 분할을 늘린다. 7은 파티션 수와 같으므로 Spark 기본
동작과 정확히 같다. 즉 드라이버 기동 시 py4j로 JVM에 들어가 `AdminClient`를 만들고
`describeTopics().all().get()`을 블로킹으로 기다린 뒤, 얻은 값으로 기본값과 동일한 옵션을 세팅하고 있다.

### 설계

`_resolve_min_partitions`까지 통째로 없애고 직접 지정 경로만 남긴다.

- `spark_job/stream_kafka.py`
  - `_fetch_kafka_partition_count` (74-106) 삭제 — 33줄
  - `_resolve_min_partitions` (62-72) 삭제 — 11줄
  - `_partition_count_cache` 필드(20-22) 삭제, `dataclasses` import에서 `field` 제거
  - 57-59줄을 다음으로 치환

    ```python
    if self.settings.kafka_min_partitions:
        reader = reader.option("minPartitions", str(self.settings.kafka_min_partitions))
    ```

- `spark_job/stream_settings.py`
  - `kafka_min_partitions_multiplier` 필드(29)와 로더(52-53) 삭제,
    `get_env_float`가 다른 곳에서도 쓰이는지 확인 후 미사용이면 import 정리
  - `kafka_min_partitions`는 유지 — 스큐 완화가 실제로 필요해질 때의 수동 손잡이다
- `docker-compose.yml`
  - 229줄 `SPARK_KAFKA_MIN_PARTITIONS_MULTIPLIER` 삭제

`SPARK_KAFKA_MIN_PARTITIONS_MULTIPLIER`는 README/ARCHITECTURE에 언급이 없어 문서 수정은 없다.

### 부수 효과

기동 시 드라이버가 Kafka에 AdminClient 연결을 하나 덜 연다. 기존 코드는 실패를 `except Exception`으로
삼키고 경고만 남겼으므로 실패해도 조용했지만, 브로커가 느릴 때 `.get()`에서 무기한 대기할 수 있는
경로였다. 제거로 그 대기 가능성도 사라진다.

---

## 12. `parse_duration_seconds` 제거 및 트리거 간격 env 초 단위 전환

### 소비처

- `spark_job/stream_kafka.py:113` — `_compute_max_offsets_per_trigger`가 초 값을 쓰는 유일한 곳
- `spark_job/fact/writers/fact_writer.py:48` — `trigger_processing_time=settings.trigger_interval`로
  **문자열 그대로** 넘어가고, `writer_base.py:123-124`에서
  `writer.trigger(processingTime=...)`에 전달된다. Spark API가 `"4 seconds"` 형태의 문자열을 요구한다.

즉 env를 정수로 바꾸면 파서는 사라지지만 Spark에 넘길 문자열은 어딘가에서 만들어야 한다.

### 설계

- `spark_job/fact/settings.py`
  - `trigger_interval: str | None` → `trigger_interval_sec: int | None`
  - 로더: `get_env_str(source, "SPARK_FACT_TRIGGER_INTERVAL")`
    → `get_env_int(source, "SPARK_FACT_TRIGGER_INTERVAL_SEC")`
- `spark_job/fact/writers/fact_writer.py:48`

  ```python
  trigger_processing_time=(
      f"{settings.trigger_interval_sec} seconds"
      if settings.trigger_interval_sec
      else None
  ),
  ```

  `writer_base`의 파라미터 타입은 건드리지 않는다. 그것은 Spark API 모양의 문자열이고
  다른 스트림도 쓰는 공용 함수다.
- `spark_job/stream_kafka.py:113`
  `trigger_seconds = parse_duration_seconds(self.fact_settings.trigger_interval)`
  → `trigger_seconds = self.fact_settings.trigger_interval_sec`
  이후 `if not trigger_seconds:` 가드가 `0`/`None`을 그대로 걸러 준다.
- `spark_job/stream_utils.py`
  - `parse_duration_seconds` (19-62) 삭제 — 44줄, `import re`도 함께 제거
  - `read_eps_from_profile`과 `yaml` import는 유지
- `spark_job/stream_kafka.py:11` import에서 `parse_duration_seconds` 제거
- `docker-compose.yml:231`
  `SPARK_FACT_TRIGGER_INTERVAL=${SPARK_FACT_TRIGGER_INTERVAL:-4 seconds}`
  → `SPARK_FACT_TRIGGER_INTERVAL_SEC=${SPARK_FACT_TRIGGER_INTERVAL_SEC:-4}`

### 문서 수정 대상

`SPARK_FACT_TRIGGER_INTERVAL` 이름이 나오는 곳: `README.md` 167·185, `ARCHITECTURE.md` 237,
`docs/tech_stack_rationale.md` 96, `docs/e2e_pipeline_flow.md` 569·674,
`docs/oom_prevention_checklist.md` 58.

이 중 `docs/e2e_pipeline_flow.md`와 `docs/oom_prevention_checklist.md`는 운영 설명 문서이므로 갱신한다.
튜닝 실험 기록(`docs/spark_tuning_log.md`)처럼 특정 시점의 설정을 남긴 이력 문서는 고치지 않는다 —
과거 실험이 어떤 값으로 돌았는지가 기록의 목적이다.

### 값 손실 확인

기존 파서는 ms/분/시간 단위도 받았지만, 실제로 들어온 값은 `"4 seconds"` 한 형태뿐이고
compose 기본값도 그것이다. 초 미만 트리거는 이 파이프라인에서 의미가 없고(현재 배치가 4초 주기로도
CPU가 포화 근처), 분·시간 단위는 near-real-time 목적과 어긋난다. 정수 초로 좁혀도 손실 없다.

---

## 13. watchdog 수동 env 파서 및 구버전 폴백 제거

### 감사 전제 정정 두 가지

1. **파서가 읽는 파일은 존재한 적이 없다.**
   `config.py:8`은 `infra/monitor/watchdog.env`를 찾는데, 저장소에도 없고 `.gitignore`가 감추는
   경로도 아니다(`.gitignore` 138줄 `.env`). 반면 `infra/monitor/README.md:21-22`는
   `infra/monitor/.env`를 `set -a && source`로 셸에 올리라고 안내한다. **파일 이름이 다르다.**
   따라서 이 11줄은 대체 대상이 아니라 처음부터 한 번도 실행된 적 없는 죽은 코드다.
2. **compose `env_file`은 적용 대상이 아니다.**
   watchdog은 compose 서비스가 아니다. 호스트에서 `python3 infra/monitor/main.py`로 돌거나
   systemd 유닛으로 뜬다(README 55-73). 컨테이너가 없으므로 `env_file`을 걸 곳이 없다.
   `python-dotenv`도 반려한다 — `infra/monitor`에는 `requirements.txt`가 없고 현재 표준 라이브러리만
   쓴다. 실행된 적 없는 기능을 되살리려고 이 패키지에 첫 외부 의존성을 들이는 것은 손해다.

### 설계

- `infra/monitor/watchdog/config.py`
  - 8-18줄(`ENV_PATH` 블록) 삭제, `from pathlib import Path` 삭제(다른 사용처 없음)
  - `import os`, `import re`는 유지
  - 구버전 폴백 3개 제거

    ```python
    P95_PRODUCER_TO_KAFKA_MS_MAX = int(os.getenv("P95_PRODUCER_TO_KAFKA_MS_MAX", "60000"))
    P95_KAFKA_TO_SPARK_INGEST_MS_MAX = int(os.getenv("P95_KAFKA_TO_SPARK_INGEST_MS_MAX", "60000"))
    P95_SPARK_TO_STORED_MS_MAX = int(os.getenv("P95_SPARK_TO_STORED_MS_MAX", "60000"))
    ```

    `P95_QUEUE_MS_MAX`·`P95_PUBLISH_MS_MAX`·`P95_SINK_MS_MAX`는 저장소 어디에도 설정되지 않고
    문서에도 없다. 기본값이 셋 다 `60000`으로 같아 폴백이 제거돼도 값이 변하지 않는다.
  - `CH_USER`/`CH_PASSWORD`의 `CLICKHOUSE_USER`/`CLICKHOUSE_PASSWORD` 폴백(53-54)은 **남긴다.**
    이 둘은 `.env`에 실제로 존재하고 compose가 쓰는 이름이라 하위호환이 아니라 현행 경로다.
- `infra/monitor/README.md`
  - 20줄 `cd /home/kang/log-etlm` — 존재하지 않는 경로. 저장소 루트 기준 상대 경로로 수정
  - 34줄 "DLQ 비율", 53줄 `DLQ_RATE_PCT_MAX` 삭제 — DLQ 파이프라인은 `83a33e2`에서 제거됐고
    해당 상수는 `config.py`에도 남아 있지 않다
  - 환경변수를 `.env` + `set -a && source`로 주입한다는 점을 "환경 변수" 절 머리에 한 줄로 명시

---

## 커밋 분리

코드와 그 값을 선언하는 compose 줄은 한 커밋에 넣는다 — 나뉘면 중간 상태에서 설정이 어긋난다.
문서는 별도 커밋으로 뺀다.

| # | 커밋 | 대상 |
| --- | --- | --- |
| 1 | `refactor(simulator): QueueThrottleConfig 제거하고 SimulatorSettings 직접 사용` | settings.py, stream_helpers.py, stream_pipeline.py, task_builder.py |
| 2 | `refactor(monitor): watchdog 임계값 점검 로직 _check_threshold로 통합` | clickhouse_probe.py |
| 3 | `refactor(spark): Kafka 파티션 수 조회 제거하고 minPartitions 직접 지정` | stream_kafka.py, stream_settings.py, docker-compose.yml |
| 4 | `refactor(spark): 트리거 간격 env를 초 단위 정수로 변경` | fact/settings.py, fact_writer.py, stream_kafka.py, stream_utils.py, docker-compose.yml |
| 5 | `refactor(monitor): watchdog 미사용 env 파서·구버전 폴백 제거` | watchdog/config.py |
| 6 | `docs: 트리거 간격 env 이름 변경 및 watchdog 문서 정정 반영` | README.md, ARCHITECTURE.md, docs/*, infra/monitor/README.md |

순서는 1~5가 서로 독립이라 임의로 가능하다. 6번은 4·5번 이후에 올린다.

## 검증

- 1·2번은 동작 동등 리팩터링이다. 시뮬레이터는 기동 후 큐 점유율이 임계에 닿을 때
  기존과 같은 백프레셔 로그가 나오는지로 확인한다.
- 3·4번은 Spark 재기동 후 드라이버 로그의 `maxOffsetsPerTrigger(auto)=` 값이 변경 전과 같은지
  확인한다(트리거 4초 × target_eps × safety로 계산되므로 12번 전환이 잘못되면 이 값이 달라진다).
- 5번은 `python3 infra/monitor/main.py`가 import 에러 없이 뜨는지만 보면 된다.
