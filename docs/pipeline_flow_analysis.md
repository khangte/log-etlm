# Kafka → Spark → ClickHouse 처리 흐름 분석

이 문서는 파이프라인의 데이터 처리 흐름을 계층별로 정리하고,
코드 분석 과정에서 확인된 설계 트레이드오프와 이질적인 부분을 기록한다.

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

---

### 3-3. `isEmpty()` 호출 방식이 계층별로 다르다

**위치**: `spark_job/clickhouse/writer_base.py`, `spark_job/dlq/writers/kafka_writer.py`

```python
# writer_base: DataFrame.isEmpty() — persist 후 호출
working_df.persist()
if skip_empty and working_df.isEmpty(): ...

# kafka_writer: rdd.isEmpty() — persist 없이 호출
if batch_df.rdd.isEmpty(): ...
```

`rdd.isEmpty()`는 불필요한 RDD 변환을 거치며, persist 없이 호출하면
이후 실제 처리에서 action이 한 번 더 실행된다.

**현재 영향**: 성능상 미미한 차이. DLQ 배치는 정상 배치보다 소량이라 실질적 문제는 없다.

**개선 방향**: `kafka_writer`도 `DataFrame.isEmpty()`로 통일한다.

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

**현재 영향**: PoC 환경에서는 허용 범위다. 단일 VM이라 ClickHouse 재시작이 드물고,
MergeTree의 async insert 버퍼 손실은 실운영 기준으로도 매우 드문 케이스다.

**개선 방향**: 정합성이 중요한 경우 `wait_for_async_insert=1`로 변경하거나,
배치 가드 기록을 `wait_for_async_insert` 완료 확인 후로 이동한다.
단, throughput과 지연의 트레이드오프가 생긴다.

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
- `SPARK_ENABLE_DLQ_STREAM=false`(기본값)일 때는 이 경로 전체가 비활성화된다.

**현재 영향**: 단일 VM PoC에서는 분리 이점이 없다. 현재 구조가 더 단순하다.

**개선 방향**: 운영 환경으로 확장할 경우 DLQ consumer를 별도 Spark job으로 분리하여
장애 격리와 독립 스케일링을 확보한다.

---

## 4. 요약

| 항목 | 심각도 | 현재 영향 | 개선 필요 시점 |
|---|---|---|---|
| ~~`time.sleep()` in foreachBatch~~ | ~~중간~~ | ✅ 해결 — sleep 제거, 즉시 재시도로 변경 | — |
| ~~파티션 조정 이중화~~ | ~~낮음~~ | ✅ 해결 — 환경변수 분리로 역할 명확화 | — |
| `rdd.isEmpty()` 비일관 | 낮음 | 성능 영향 미미 | 코드 정리 시 |
| async insert + 배치 가드 충돌 | 중간 | PoC 허용 범위 | 운영 전환 전 |
| DLQ produce/consume 동일 job | 설계 | 단일 VM에서 무해 | 운영 스케일아웃 전 |
