# 파이프라인 문제 해결 리스트

이 문서는 파이프라인 코드 분석 과정에서 발견된 문제와 설계 트레이드오프를 기록한다.
완료된 항목은 ✅, 미완료는 🔧, 설계 결정으로 수용한 항목은 ⚠️ 로 표시한다.

최종 수정: 2026-05-19

---

## 완료

### ✅ `time.sleep()`이 driver 스레드를 블로킹한다

**위치**: `spark_job/clickhouse/sink.py`

`foreachBatch`는 Spark driver 스레드에서 실행된다. 이 안에서 `time.sleep()`으로 backoff를
구현하면 해당 스트리밍 쿼리의 다음 트리거가 sleep 시간만큼 밀린다.

**변경 내용**:
- `time.sleep()` 및 `retry_backoff_sec` 필드 제거
- 재시도 시 즉시 재시도(sleep 없음)로 변경 — driver 스레드 블로킹 없음
- backoff가 필요한 수준의 장애라면 예외를 올려 Spark 체크포인트 기반 재처리를 활용
- `SPARK_CLICKHOUSE_RETRY_BACKOFF_SEC` 환경변수 `docker-compose.yml`에서 제거

---

### ✅ 파티션 조정이 두 계층에 중복 존재한다

**위치**: `spark_job/clickhouse/writer_base.py`, `spark_job/clickhouse/sink.py`

두 coalesce는 목적이 달랐으나 같은 환경변수(`SPARK_CLICKHOUSE_WRITE_PARTITIONS`)를 공유해 혼란이 있었다.

| 계층 | 목적 |
|---|---|
| `writer_base` (`pre_coalesce_partitions`) | foreachBatch 진입 직후 Spark 내부 처리 비용 절감 |
| `sink.py` (`write_partitions`) | JDBC write 직전 ClickHouse 동시 커넥션 수(Insert 부하) 제어 |

**변경 내용**: 환경변수를 분리해 각 계층의 역할을 명확히 구분했다.

- `SPARK_FACT_PRE_COALESCE_PARTITIONS` → `writer_base` coalesce (Spark 처리용, 기본값 3)
- `SPARK_CLICKHOUSE_WRITE_PARTITIONS` → `sink.py` `_apply_partitioning` (JDBC 커넥션 수 제어용, 기본값 3)
- `FactStreamSettings.num_partitions` → `pre_coalesce_partitions`로 필드명 변경

두 값이 같을 때 1단계 coalesce 후 파티션 수가 확정됨에도 2단계 `_apply_partitioning`이
매 배치마다 `rdd.getNumPartitions()` action을 유발하는 문제도 함께 제거했다.
1단계에서 추적한 `current_parts`를 `write_to_clickhouse(current_partitions=…)`로 전달해
스트리밍 경로에서는 2단계의 `getNumPartitions()` 호출을 생략한다.

---

### ✅ `isEmpty()` 호출 방식이 계층별로 다르다

**위치**: `spark_job/clickhouse/writer_base.py`, `spark_job/dlq/writers/kafka_writer.py`

**변경 내용**: `kafka_writer`의 `batch_df.rdd.isEmpty()`를 `batch_df.isEmpty()`로 통일했다.

- `rdd.isEmpty()`는 불필요한 RDD 변환을 거치며, persist 없이 호출하면 이후 실제 처리에서 action이 한 번 더 실행된다.
- 두 계층 모두 `DataFrame.isEmpty()`로 일관성을 확보했다.

---

### ✅ `build_batch_messages_from_simulator()`가 asyncio 이벤트 루프를 블로킹한다

**위치**: `log_simulator/simulator/stream_pipeline.py`

`run_simulator_loop`는 asyncio 코루틴이지만, 배치 생성 단계에서
`build_batch_messages_from_simulator()`를 동기 함수로 직접 호출했다.
이 함수는 `generate_events_one()` + `render_bytes()`를 count(최대 2000)번 반복하는
pure-Python CPU 루프로, 실행 시간이 0.7~1.9s에 달했다.

asyncio 이벤트 루프는 단일 스레드이므로 이 시간 동안 publisher worker 코루틴이
전혀 스케줄되지 못했다. 결과적으로 큐에 아이템이 있음에도 워커가 15~21초씩
대기(`idle worker wait=17s queue=2`)하고, EPS가 목표치의 5~10% 수준으로 폭락했다.

**변경 내용**:

```python
# 변경 전
batch_items = build_batch_messages_from_simulator(simulator, service, batch_size)

# 변경 후
batch_items = await asyncio.to_thread(
    build_batch_messages_from_simulator, simulator, service, batch_size
)
```

`asyncio.to_thread()`로 스레드 풀에 오프로드해 CPU 작업 중에도 이벤트 루프가
publisher worker를 스케줄할 수 있도록 했다.

---

## 설계 결정 (수용)

### ⚠️ async insert와 배치 가드의 의미가 충돌한다

**위치**: `infra/clickhouse/users.d/zz-log_user-async-profile.xml`, `spark_job/clickhouse/sink.py`

```xml
<async_insert>1</async_insert>
<wait_for_async_insert>0</wait_for_async_insert>  <!-- fire-and-forget -->
```

`wait_for_async_insert=0`이므로 JDBC `.save()`가 반환되는 시점은 ClickHouse가 데이터를
**버퍼에 수신한 것**이지 **디스크에 flush한 것**이 아니다.
이 상태에서 배치 가드를 기록하면, flush 이전에 ClickHouse가 재시작될 경우
"데이터 유실 + 가드 성공 기록" 상태가 되어 재처리가 불가능해진다.

**설계 선택**: `wait_for_async_insert=0`은 의도적인 결정이다.
이 파이프라인은 정합성보다 **처리량(throughput)을 우선**하며,
ClickHouse flush 완료를 기다리는 latency 비용을 감수하지 않는다.

- 허용되는 위험: flush 직전 ClickHouse 재시작 시 데이터 유실 + 가드 성공 기록 → 재처리 불가
- 수용 근거: MergeTree의 async insert 버퍼 손실은 실운영 기준으로도 매우 드문 케이스이며, PoC 단일 VM 환경에서는 더욱 드물다

**운영 전환 시 고려사항**: 정합성 요구가 높아지면 `wait_for_async_insert=1`로 변경하거나
배치 가드 대신 `ReplacingMergeTree` + `event_id` 기반 멱등 적재로 전환한다.

---

### ⚠️ DLQ produce / consume이 같은 Spark job 안에 있다

**위치**: `spark_job/stream_ingest.py`

같은 Spark job이 `logs.dlq`에 쓰는 스트림과 읽는 스트림을 동시에 운영한다.
두 쿼리는 독립적으로 실행되므로 기능적 문제는 없으나 다음 제약이 생긴다.

- DLQ consumer만 독립 재시작하거나 별도 스케일링이 불가능하다.
- fact_event 스트림 장애가 DLQ consumer에도 영향을 준다.

**설계 선택**: 현행 구조를 유지한다.

- `SPARK_ENABLE_DLQ_STREAM=false`(기본값)로 DLQ 스트림 경로 자체가 비활성화되어 있다.
- 단일 VM PoC에서는 분리 이점이 없으며, 현재 구조가 더 단순하다.
- 운영 환경으로 확장 시 DLQ consumer를 별도 Spark job으로 분리해 장애 격리와 독립 스케일링을 확보한다.

---

## 미완료 — 처리 속도·효율 개선

직렬화 포맷(JSON)과 전체 구조는 유지한 채로 적용 가능한 개선 항목.
우선순위는 예상 효과 기준이며, 단일 VM(vCPU 7) 제약을 전제로 한다.

---

### 🔧 [Simulator] `behind target` — `render_bytes` 직렬화 병목

**위치**: `log_simulator/simulator/base.py`, `log_simulator/requirements.txt`

**원인**: `render_bytes()`가 표준 `json.dumps()`를 사용한다.
`asyncio.to_thread`로 이벤트 루프 블로킹은 해소했으나,
스레드 내에서 Python GIL을 점유한 채로 직렬화가 진행되어 생성 시간이 여전히 길다.

```
현재: json.dumps(log, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
```

**개선 옵션 (우선순위 순)**:

| 순위 | 방법 | 효과 | 비고 |
|------|------|------|------|
| 1 | `orjson` 교체 | 직렬화 3~10배 빠름, Rust 구현으로 GIL 해제 | 변경 2줄, 리스크 없음 |
| 2 | Python 3.13 free-threaded 빌드 | `asyncio.to_thread`가 진짜 병렬 실행 | `python:3.13t-slim`으로 base image 교체 |
| 3 | `ProcessPoolExecutor` 전환 | 멀티코어 완전 활용, GIL 우회 | picklable 검증 + `cpus` 증설 필요 |

> **참고**: `LOOPS_PER_SERVICE` 증가는 Python GIL + `cpus: "1.0"` 환경에서 효과 없음 (스레드 경합만 증가).
> `LOG_BATCH_SIZE=2000`은 백프레셔 해제 시 carry 스파이크 상한이므로, 단독 개선 효과는 제한적.

**1순위 적용 방법**:

```python
# log_simulator/simulator/base.py
import orjson  # 추가

def render_bytes(self, log: Dict[str, Any]) -> bytes:
    return orjson.dumps(log)
```

```
# log_simulator/requirements.txt
orjson>=3.9,<4.0
```

**난이도**: 낮 | **예상 효과**: 높음

---

### 🔧 [Spark] `falling behind` — JVM 콜드 스타트

**위치**: Spark Structured Streaming 내부

**원인**: Spark 재기동 직후 첫 2~3 배치에서 발생하는 구조적 현상.
JVM JIT 컴파일 + ClickHouse 커넥션 풀 초기화가 겹쳐 18~21s 소요.
약 15 배치 이내 자동으로 정상화 (steady-state: 1.5~2s / trigger: 4s).

```
batch+0: 18.1s  ← falling behind
batch+1: 21.4s  ← falling behind
batch+2:  8.9s
...
batch+14: 1.8s  ← 정상화
```

**현행 설정 유지 근거**:
- `SPARK_MAX_OFFSETS_SAFETY=1.1` → maxOffsets(22,000) > 생산량(20,000/4s) → 10% 여유로 따라잡기 가능
- safety를 1.0 미만으로 낮추면 소비 < 생산 → Kafka lag 영구 누적
- 트리거 간격을 늘리면 maxOffsets 증가로 초기 배치 처리량이 더 커져 역효과
- **설정 변경 불필요**: 재기동마다 반복되나 15 배치(약 1분) 내 자동 해소

**운영 전환 시 고려사항**: 콜드 스타트를 줄이려면 Spark JVM 프로세스를 유지한 채
스트리밍 쿼리만 재시작하거나, 초기 N 배치 동안 낮은 maxOffsets를 적용하는
warmup 로직을 추가하는 방향으로 접근한다.

**난이도**: 해당 없음 (설정 변경 없음) | **예상 효과**: 해당 없음

---

### 🔧 ClickHouse Native Connector로 JDBC 대체

**위치**: `spark_job/clickhouse/sink.py`, `spark_job/clickhouse/settings.py`

현재 `com.clickhouse.jdbc.ClickHouseDriver`를 통한 JDBC write는
ClickHouse의 columnar bulk insert 강점을 활용하지 못한다.
`spark-clickhouse-connector`는 `FORMAT Native` 또는 `FORMAT RowBinary`로
직렬화해 JDBC 대비 3~5배 throughput을 낼 수 있다.

```python
# 현재
out_df.write.format("jdbc").option("driver", "com.clickhouse.jdbc.ClickHouseDriver")...

# 개선
out_df.write.format("clickhouse").option("clickhouse.url", ...)...
```

적용 범위: `build_jdbc_options()` 대체 + JAR 교체 (`spark-clickhouse-connector_*.jar`).
`write_to_clickhouse()` 바깥 로직(배치 가드, DLQ, 재시도)은 변경 불필요.

**비교 방법**:

```bash
# 변경 전 write duration 평균 (5분 분량)
grep "stream=fact_event.*stage=write" /data/log-etlm/spark-events/batch_timing.log \
  | tail -80 \
  | grep -oP 'duration=\K[0-9.]+' \
  | awk '{sum+=$1; n++} END {printf "avg=%.3fs n=%d\n", sum/n, n}'
```

| 지표 | 확인 위치 | 기대 변화 |
|---|---|---|
| `stage=write` duration | `batch_timing.log` | 50~70% 단축 |
| Avg Processing Rate | Spark UI Streaming | 증가 |
| spark-driver CPU % | `docker stats spark-driver` | 감소 |

**난이도**: 중 | **예상 효과**: 높음

---

### 🔧 Watermark 단축으로 Spark 상태 저장소 축소

**위치**: `docker-compose.yml` → `SPARK_FACT_DEDUP_WATERMARK`

현재 `1 hour` watermark + 높은 EPS면 Spark 상태 저장소(기본 in-memory)에
수백만 건의 `event_id`가 누적되어 GC 압박을 일으킬 수 있다.

```
SPARK_FACT_DEDUP_WATERMARK=10 minutes
```

1시간 이내 중복 재전송은 PoC 시나리오에서 사실상 없으므로 기준 완화가 안전.
상태 크기 감소 → GC 빈도 감소 → 트리거 지연 안정화.

**비교 방법**:

```bash
# Spark Structured Streaming 상태 크기
# localhost:4040 → Structured Streaming → fact_event_stream → "State Rows Total"

# 체크포인트 디렉터리 내 state 크기
du -sh /data/log-etlm/spark_checkpoints/fact_event/state/
```

| 지표 | 확인 위치 | 기대 변화 |
|---|---|---|
| State Rows Total | Spark UI Streaming | 수렴값 감소 |
| state/ 디렉터리 크기 | `du -sh .../state/` | 감소 |
| batchDuration 편차 | Spark UI Streaming | 편차 감소 |

**난이도**: 낮 | **예상 효과**: 중

---

### 🔧 `raw_json` 컬럼 조건부 생성 제거

**위치**: `spark_job/fact/transforms/parse_event.py`, `normalize_event.py`

`SPARK_STORE_RAW_JSON=false`(현재 기본값)일 때도 `parse_event`에서
`raw_json` 컬럼을 항상 생성하고 `normalize_event`에서 `F.lit("")`로 교체한다.
컬럼 생성 → 셔플 전달 → 빈 문자열 교체 순서로 불필요한 데이터가 파티션을 이동한다.

개선 방향: `store_raw_json` 플래그를 `parse_event`까지 내려보내
`false`일 때 `raw_json` 컬럼 자체를 만들지 않는다.

**비교 방법**:

```bash
# Spark UI: localhost:4040 → Stages → Shuffle Write bytes 변경 전/후 비교
# (raw_json 평균 크기 ~300B × EPS × 300s = 대략적인 감소 예상치)
```

**난이도**: 낮 | **예상 효과**: 낮~중

---

### 🔧 빈 배치 근본 억제 — `maxOffsetsPerTrigger` 정밀 조정

**위치**: `docker-compose.yml` → `SPARK_MAX_OFFSETS_PER_TRIGGER`

`SPARK_SKIP_EMPTY_BATCH=true`로 빈 배치를 skip하면 배치마다
`persist() → isEmpty() → unpersist()` 흐름으로 추가 Spark Job이 발생한다.

빈 트리거 자체를 줄이려면 Kafka lag 모니터링 결과를 기준으로
`maxOffsetsPerTrigger`를 `target_eps × trigger_seconds × 1.1` 수식으로
실측값에 맞게 고정하는 것이 근본 해결책이다.

```
SPARK_MAX_OFFSETS_PER_TRIGGER=<실측 EPS × trigger_sec × 1.1>
SPARK_SKIP_EMPTY_BATCH=false   # 빈 배치가 줄면 불필요
```

**비교 방법**:

```bash
# 1) 현재 Kafka lag 확인
python3 scripts/kafka_spark_lag.py

# 2) Spark UI → Structured Streaming → fact_event_stream
# "Num Input Rows" 가 0인 배치 비율 확인
```

**난이도**: 낮 | **예상 효과**: 낮~중

---

## 우선순위 요약

| 순위 | 항목 | 난이도 | 예상 효과 | 상태 |
|------|------|--------|-----------|------|
| 1 | Simulator `render_bytes` → orjson | 낮 | 높음 | 🔧 |
| 2 | ClickHouse Native Connector | 중 | 높음 | 🔧 |
| 3 | Watermark 단축 (`10 minutes`) | 낮 | 중 | 🔧 |
| 4 | `raw_json` 조건부 제거 | 낮 | 낮~중 | 🔧 |
| 5 | `maxOffsetsPerTrigger` 정밀 조정 | 낮 | 낮~중 | 🔧 |
| — | Spark `falling behind` | — | — | 설정 변경 불필요 |
