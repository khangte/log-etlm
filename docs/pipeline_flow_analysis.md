# Kafka → Spark → ClickHouse 처리 흐름 분석

이 문서는 파이프라인의 데이터 처리 흐름을 계층별로 정리하고,
코드 분석 과정에서 확인된 설계 트레이드오프와 이질적인 부분을 기록한다.

일자: 2026-05-19

---

## 1. 전체 처리 흐름

```
Kafka (logs.auth / logs.order / logs.payment / logs.error)
  │
  │  [maxOffsetsPerTrigger, minPartitions]
  ▼
readStream.format("kafka")                   ← KafkaStreamBuilder
  │
  ▼
parse_event()                                ← Kafka raw → from_json, spark_ingest_ts 부여
  ▼
validate_event()                             ← json IS NULL 기준 good_df / bad_df 분리
  ▼
normalize_event()                            ← 컬럼 정규화, 타임스탬프 계산
  ▼
withWatermark("event_ts") +
dropDuplicatesWithinWatermark(["event_id"])  ← 상태 기반 dedup
  ▼
foreachBatch(_foreach)
  ├─ coalesce(pre_coalesce_partitions)
  ├─ skip_empty 체크  (persist → isEmpty → unpersist)
  ├─ dropDuplicates  (배치 내 dedup; watermark dedup 사용 시 생략)
  ├─ processed_ts / process_ms 재계산
  └─ write_to_clickhouse()
       ├─ batch_guard 중복 체크  (JDBC → analytics.stream_batch_guard)
       ├─ _apply_partitioning   (coalesce 또는 repartition)
       ├─ JDBC write            → analytics.fact_event
       ├─ batch_guard 기록
       └─ 최종 실패 시          → DLQ Kafka (logs.dlq)

bad_df (JSON 파싱 실패)
  └─ KafkaDlqWriter            → logs.dlq (produce)
       └─ readStream(logs.dlq)
            └─ parse_dlq → ClickHouseDlqWriter
                 └─ analytics.fact_event_dlq
```

---

## 2. 타임스탬프 흐름

| 컬럼 | 의미 | 생성 위치 |
|---|---|---|
| `created_ts` | 이벤트 생성 시각 (`ts_ms` 기반) | `normalize_event` |
| `event_ts` | dedup / watermark 기준 (= `created_ts`) | `normalize_event` |
| `ingest_ts` | Kafka 도착 시각 (= `kafka_ts`) | `normalize_event` |
| `kafka_ts` | Kafka timestamp | `parse_event` |
| `spark_ingest_ts` | Spark 파싱 시각 | `parse_event` |
| `processed_ts` | `foreachBatch` 직전 재계산 | `writer_base._foreach` |
| `stored_ts` | ClickHouse `DEFAULT now64(3)` | ClickHouse DDL |

지연 지표 계산식:

- `ingest_ms` = `ingest_ts` − `event_ts`
- `process_ms` = `processed_ts` − `spark_ingest_ts`
- E2E = `stored_ts` − `ingest_ts`

---

## 3. 설계 트레이드오프 및 이질적 부분

### 3-1. ~~`time.sleep()`이 driver 스레드를 블로킹한다~~ ✅ 해결됨

**위치**: `spark_job/clickhouse/sink.py`

`foreachBatch`는 Spark driver 스레드에서 실행된다. 이 안에서 `time.sleep()`으로 backoff를
구현하면 해당 스트리밍 쿼리의 다음 트리거가 sleep 시간만큼 밀린다.

**변경 내용**:
- `time.sleep()` 및 `retry_backoff_sec` 필드 제거
- 재시도 시 즉시 재시도(sleep 없음)로 변경 — driver 스레드 블로킹 없음
- backoff가 필요한 수준의 장애라면 예외를 올려 Spark 체크포인트 기반 재처리를 활용
- `SPARK_CLICKHOUSE_RETRY_BACKOFF_SEC` 환경변수 `docker-compose.yml`에서 제거

---

### 3-2. ~~파티션 조정이 두 계층에 중복 존재한다~~ ✅ 해결됨

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

**추가 개선 (no-op `getNumPartitions()` 제거)**:

두 값이 같을 때(기본값 `pre=3, write=3`) 1단계 coalesce 후 이미 파티션 수가 확정됨에도
2단계 `_apply_partitioning`이 매 배치마다 `rdd.getNumPartitions()` action을 유발하는 문제를 제거했다.

1단계에서 추적한 `current_parts`를 `write_to_clickhouse(current_partitions=…)`로 전달해
스트리밍 경로에서는 2단계의 `getNumPartitions()` 호출을 생략한다.
배치 경로(`write_batch`)는 `current_partitions=None`으로 기존 동작을 유지한다.

---

### 3-3. ~~`isEmpty()` 호출 방식이 계층별로 다르다~~ ✅ 해결됨

**위치**: `spark_job/clickhouse/writer_base.py`, `spark_job/dlq/writers/kafka_writer.py`

**변경 내용**: `kafka_writer`의 `batch_df.rdd.isEmpty()`를 `batch_df.isEmpty()`로 통일했다.

- `rdd.isEmpty()`는 불필요한 RDD 변환을 거치며, persist 없이 호출하면 이후 실제 처리에서 action이 한 번 더 실행된다.
- 두 계층 모두 `DataFrame.isEmpty()`로 일관성을 확보했다.

---

### 3-4. async insert와 배치 가드의 의미가 충돌한다

**위치**: `infra/clickhouse/users.d/zz-log_user-async-profile.xml`, `spark_job/clickhouse/sink.py`

```xml
<async_insert>1</async_insert>
<wait_for_async_insert>0</wait_for_async_insert>  <!-- fire-and-forget -->
```

```python
writer.save()        # JDBC 성공 = ClickHouse 버퍼 수신 완료
data_written = True  # 디스크 flush는 아직 보장되지 않음
# → batch_guard 기록
```

`wait_for_async_insert=0`이므로 JDBC `.save()`가 반환되는 시점은 ClickHouse가 데이터를
**버퍼에 수신한 것**이지 **디스크에 flush한 것**이 아니다.
이 상태에서 배치 가드를 기록하면, flush 이전에 ClickHouse가 재시작될 경우
"데이터 유실 + 가드 성공 기록" 상태가 되어 재처리가 불가능해진다.

부가적으로, `stored_ts DEFAULT now64(3)`도 async insert 버퍼 수신 시각을 찍으므로
E2E 지연 측정에서 실제 디스크 반영 시각과 차이가 있을 수 있다.

**설계 선택**: `wait_for_async_insert=0`은 의도적인 결정이다.
이 파이프라인은 정합성보다 **처리량(throughput)을 우선**하며,
ClickHouse flush 완료를 기다리는 latency 비용을 감수하지 않는다.

- 허용되는 위험: flush 직전 ClickHouse 재시작 시 데이터 유실 + 가드 성공 기록 → 재처리 불가
- 수용 근거: MergeTree의 async insert 버퍼 손실은 실운영 기준으로도 매우 드문 케이스이며, PoC 단일 VM 환경에서는 더욱 드물다

**운영 전환 시 고려사항**: 정합성 요구가 높아지면 `wait_for_async_insert=1`로 변경하거나
배치 가드 대신 `ReplacingMergeTree` + `event_id` 기반 멱등 적재로 전환한다.
단, 전자는 insert latency 증가, 후자는 배치 가드 로직 전체 제거가 필요하다.

---

### 3-6. `build_batch_messages_from_simulator()`가 asyncio 이벤트 루프를 블로킹한다 ✅ 해결됨

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

### 3-5. DLQ produce / consume이 같은 Spark job 안에 있다

**위치**: `spark_job/stream_ingest.py`

```python
def _run_dlq_streams(self, spark, bad_df):
    self.dlq_kafka_writer.write_dlq_kafka_stream(bad_df, topic=dlq_topic)  # produce
    dlq_source = self._build_kafka_stream(spark, dlq_topic)                # consume
    dlq_df = build_dlq_stream_df(dlq_source)
    self.dlq_writer.write_dlq_stream(dlq_df)
```

같은 Spark job이 `logs.dlq`에 쓰는 스트림과 읽는 스트림을 동시에 운영한다.
두 쿼리는 독립적으로 실행되므로 기능적 문제는 없으나 다음 제약이 생긴다.

- DLQ consumer만 독립 재시작하거나 별도 스케일링이 불가능하다.
- fact_event 스트림 장애가 DLQ consumer에도 영향을 준다.

**설계 선택**: 현행 구조를 유지한다.

- `SPARK_ENABLE_DLQ_STREAM=false`(기본값)로 DLQ 스트림 경로 자체가 비활성화되어 있다.
- 단일 VM PoC에서는 분리 이점이 없으며, 현재 구조가 더 단순하다.
- 운영 환경으로 확장 시 DLQ consumer를 별도 Spark job으로 분리해 장애 격리와 독립 스케일링을 확보한다.

---

## 4. 요약

| 항목 | 심각도 | 현재 영향 | 개선 필요 시점 |
|---|---|---|---|
| ~~`time.sleep()` in foreachBatch~~ | ~~중간~~ | ✅ 해결 — sleep 제거, 즉시 재시도로 변경 | — |
| ~~파티션 조정 이중화~~ | ~~낮음~~ | ✅ 해결 — 환경변수 분리 + no-op `getNumPartitions()` 제거 | — |
| ~~`rdd.isEmpty()` 비일관~~ | ~~낮음~~ | ✅ 해결 — `DataFrame.isEmpty()`로 통일 | — |
| async insert + 배치 가드 충돌 | 중간 | throughput 우선 설계 선택 — 유실 위험 인지하고 수용 | 운영 전환 전 정합성 요구 시 |
| DLQ produce/consume 동일 job | 설계 | 단일 VM에서 무해 | 운영 스케일아웃 전 |

---

## 5. 처리 속도·효율 개선 후보

직렬화 포맷(JSON)과 전체 구조는 유지한 채로 적용 가능한 개선 항목.
우선순위는 예상 효과 기준이며, 단일 VM(vCPU 7) 제약을 전제로 한다.

### 5-1. Kafka Producer 압축 활성화

**위치**: `docker-compose.yml` → `simulator` 서비스 환경변수

현재 `PRODUCER_COMPRESSION=none`. JSON 텍스트는 압축률이 60~70%에 달해
Kafka 내부 I/O와 Spark 수신 대역폭을 동시에 줄일 수 있다.

```
PRODUCER_COMPRESSION=lz4
```

- `acks=0`이라 producer latency는 이미 최소화되어 있으므로 압축 CPU 비용이 순이익이 됨
- CPU가 포화 상태라면 오히려 역효과 — 실측 후 적용 결정
- `snappy`는 lz4보다 압축률 높으나 CPU 비용 더 큼; `zstd`는 최고 압축률이나 PoC에서 오버스펙

**난이도**: 낮 | **예상 효과**: 중 (Kafka I/O 감소)

**비교 방법**:

적용 전/후 각 5분 이상 동일 EPS로 운영한 뒤 아래 지표를 비교한다.

```bash
# 1) Kafka 토픽별 바이트 처리량 (5초 간격 샘플링)
docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
  --describe --topic logs.auth

# 2) Kafka 브로커 네트워크 수신량 (bytes-in/sec)
docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092
# kafka-ui (localhost:8080) → Brokers → Metrics → BytesInPerSec 확인

# 3) simulator 컨테이너 CPU 사용률
docker stats simulator --no-stream --format "{{.CPUPerc}} {{.MemUsage}}"
```

| 지표 | 확인 위치 | 기대 변화 |
|---|---|---|
| Kafka BytesInPerSec | kafka-ui → Brokers → Metrics | 감소 (압축된 바이트 수신) |
| simulator CPU % | `docker stats simulator` | 소폭 증가 (압축 비용) |
| Spark `stage=write` duration | `spark-events/batch_timing.log` | 유지 또는 감소 |

---

### 5-2. Watermark 단축으로 Spark 상태 저장소 축소

**위치**: `docker-compose.yml` → `SPARK_FACT_DEDUP_WATERMARK`

현재 `1 hour` watermark + 높은 EPS면 Spark 상태 저장소(기본 in-memory)에
수백만 건의 `event_id`가 누적되어 GC 압박을 일으킬 수 있다.

```
SPARK_FACT_DEDUP_WATERMARK=10 minutes
```

- 1시간 이내 중복 재전송은 PoC 시나리오에서 사실상 없으므로 기준 완화가 안전
- 상태 크기 감소 → GC 빈도 감소 → 트리거 지연 안정화

**대안**: Spark dedup을 완전히 제거하고 ClickHouse `ReplacingMergeTree` + `event_id` 기반
멱등 적재로 위임하면 상태 저장소 자체를 없앨 수 있다.
단, 배치 가드 로직 전체 교체가 필요하므로 3-4의 설계 결정과 연계해 판단한다.

**난이도**: 낮 | **예상 효과**: 중 (상태 저장소 크기·GC 부하 감소)

**비교 방법**:

상태 저장소 크기는 Spark UI Structured Streaming 탭과 체크포인트 디렉터리 크기로 확인한다.

```bash
# 1) Spark Structured Streaming 상태 크기
# localhost:4040 → Structured Streaming → fact_event_stream
# "State Rows Total" 항목이 watermark 단축 후 수렴 속도 빨라지는지 확인

# 2) 체크포인트 디렉터리 내 state 크기
du -sh /data/log-etlm/spark_checkpoints/fact_event/state/

# 3) spark-driver GC 빈도 (JVM GC 로그 활성화 시)
docker logs spark-driver 2>&1 | grep -i "GC\|pause" | tail -30

# 4) 트리거 간격 안정성: batchDuration 편차 확인
# localhost:4040 → Structured Streaming → fact_event_stream → 최근 배치 duration 분포
```

| 지표 | 확인 위치 | 기대 변화 |
|---|---|---|
| State Rows Total | Spark UI Streaming | 수렴값 감소 (EPS × 10분으로 안정화) |
| state/ 디렉터리 크기 | `du -sh .../state/` | 감소 |
| batchDuration 편차 | Spark UI Streaming | 편차 감소 (GC stop-the-world 완화) |

---

### 5-3. `raw_json` 컬럼 조건부 생성 제거

**위치**: `spark_job/fact/transforms/parse_event.py`, `normalize_event.py`

`SPARK_STORE_RAW_JSON=false`(현재 기본값)일 때도 `parse_event`에서
`raw_json` 컬럼을 항상 생성하고 `normalize_event`에서 `F.lit("")`로 교체한다.
컬럼 생성 → 셔플 전달 → 빈 문자열 교체 순서로 불필요한 데이터가 파티션을 이동한다.

개선 방향: `store_raw_json` 플래그를 `parse_event`까지 내려보내
`false`일 때 `raw_json` 컬럼 자체를 만들지 않는다.

**난이도**: 낮 | **예상 효과**: 낮~중 (셔플 데이터 감소)

**비교 방법**:

셔플 데이터 크기는 Spark UI Stage 탭의 Shuffle Read/Write 수치로 확인한다.

```bash
# Spark UI: localhost:4040 → Stages
# 변경 전: parse_event 단계 Shuffle Write에 raw_json 크기 포함
# 변경 후: Shuffle Write bytes 감소 확인

# 정량 비교: 동일 EPS 5분 운영 후 Stages 탭에서
# "Shuffle Write" 총합을 변경 전/후 캡처해 비교
# (raw_json 평균 크기 ~300B × EPS × 300s = 대략적인 감소 예상치)
```

| 지표 | 확인 위치 | 기대 변화 |
|---|---|---|
| Shuffle Write bytes | `localhost:4040` → Stages | 감소 |
| `stage=transform` duration | `batch_timing.log` | 소폭 단축 |

> raw_json이 빈 문자열(`""`)이더라도 컬럼이 존재하면 파티션 간 직렬화 비용이 발생한다.
> EPS가 낮을 때는 체감이 미미하고, EPS가 높을수록 효과가 커진다.

---

### 5-4. ClickHouse Native Connector로 JDBC 대체

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

**난이도**: 중 | **예상 효과**: 높음 (columnar bulk insert 활용)

**비교 방법**:

write duration이 핵심 지표다. `batch_timing.log`의 `stage=write` duration을 전/후 비교한다.

```bash
# 1) 변경 전 write duration 평균 (5분 분량)
grep "stream=fact_event.*stage=write" /data/log-etlm/spark-events/batch_timing.log \
  | tail -80 \
  | grep -oP 'duration=\K[0-9.]+' \
  | awk '{sum+=$1; n++} END {printf "avg=%.3fs n=%d\n", sum/n, n}'

# 2) ClickHouse 수신 행 수 (asynchronous_insert_log 기준)
docker exec clickhouse clickhouse-client \
  -u log_user --password "$CLICKHOUSE_PASSWORD" \
  --query "
    SELECT
      toStartOfMinute(event_time) AS minute,
      sum(rows) AS rows,
      formatReadableSize(sum(bytes)) AS bytes
    FROM system.asynchronous_insert_log
    WHERE table = 'fact_event'
      AND event_time >= now() - INTERVAL 10 MINUTE
    GROUP BY minute
    ORDER BY minute
    FORMAT PrettyCompact"

# 3) Spark UI → Structured Streaming → fact_event_stream
# "Avg Input Rate" 와 "Avg Processing Rate" 비교
# Processing Rate > Input Rate 이면 여유 있음
```

| 지표 | 확인 위치 | 기대 변화 |
|---|---|---|
| `stage=write` duration | `batch_timing.log` | 50~70% 단축 |
| ClickHouse 수신 rows/s | `asynchronous_insert_log` | 유지 또는 증가 |
| Avg Processing Rate | Spark UI Streaming | 증가 |
| spark-driver CPU % | `docker stats spark-driver` | 감소 (직렬화 부하 감소) |

---

### 5-5. 빈 배치 근본 억제 — `maxOffsetsPerTrigger` 정밀 조정

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

**난이도**: 낮 | **예상 효과**: 낮~중 (불필요한 Job 제거)

**비교 방법**:

Kafka lag 스크립트와 Spark UI를 함께 활용해 빈 배치 비율을 측정한다.

```bash
# 1) 현재 Kafka lag 확인 (scripts/kafka_spark_lag.py 활용)
python3 scripts/kafka_spark_lag.py

# 2) Spark UI에서 빈 배치 비율 확인
# localhost:4040 → Structured Streaming → fact_event_stream
# "Num Input Rows" 가 0인 배치 비율을 확인
# → 0인 배치가 많으면 maxOffsetsPerTrigger 또는 트리거 간격 조정 필요

# 3) 적정값 계산 공식
# target_eps = profiles.yml에서 total_eps 확인
# trigger_sec = SPARK_FACT_TRIGGER_INTERVAL (예: 4)
# max_offsets = target_eps × trigger_sec × 1.1
# 예) 500 EPS × 4s × 1.1 = 2200

# 4) 조정 후 재측정: Num Input Rows 분포가 0 없이 안정적인지 확인
```

| 지표 | 확인 위치 | 기대 변화 |
|---|---|---|
| 빈 배치(Input Rows=0) 비율 | Spark UI Streaming | 감소 또는 0 |
| Kafka consumer lag | `kafka_spark_lag.py` | 안정적 유지 (lag 누적 없음) |
| `stage=transform` duration | `batch_timing.log` | 안정화 (0행 배치 제거) |

---

### 우선순위 요약

| 순위 | 항목 | 섹션 | 난이도 | 예상 효과 |
|---|---|---|---|---|
| 1 | ClickHouse Native Connector | 5-4 | 중 | 높음 |
| 2 | Kafka lz4 압축 | 5-1 | 낮 | 중 |
| 3 | Watermark 단축 (`10 minutes`) | 5-2 | 낮 | 중 |
| 4 | `raw_json` 조건부 제거 | 5-3 | 낮 | 낮~중 |
| 5 | `maxOffsetsPerTrigger` 정밀 조정 | 5-5 | 낮 | 낮~중 |

> **즉시 적용 권장**: 5-2 (watermark 단축)은 환경변수 값 하나 변경으로 리스크 없이 적용 가능하다.
> 5-4 (Native Connector)는 가장 큰 효과를 기대할 수 있으나 JAR 교체와 설정 변경이 수반된다.
