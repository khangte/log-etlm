# 면접 예상 Q&A

이 문서는 이 파이프라인 프로젝트를 기반으로 면접에서 받을 수 있는 질문과 답변을 정리한다.  
단순 암기용이 아니라, 설계 의도와 트레이드오프를 이해한 상태에서 자신의 언어로 풀어낼 수 있도록 맥락을 함께 기록한다.

---

## 1. 왜 Kafka Streams나 Flink 대신 Spark를 선택했나?

**핵심 답변**

> "집계는 ClickHouse Materialized View에 위임하고, Spark는 Kafka 소비·파싱·정규화·적재에만 집중했습니다.  
> 이 역할 분리 덕분에 Spark 트리거 지연 없이 집계 결과가 INSERT 즉시 갱신됩니다.  
> 각 레이어가 잘하는 역할을 맡긴 설계입니다."

**선택 근거**

- **배치·스트리밍 API 통일**: Spark는 `spark-driver`(스트리밍)와 `spark-batch`(dimension 갱신)가 동일한 DataFrame API와 SparkSession을 공유한다. Flink는 Table API가 별도 생태계이고, Kafka Streams는 배치 처리 개념 자체가 없다.
- **`foreachBatch` 커스텀 싱크**: 마이크로배치 단위로 Python 코드를 직접 실행할 수 있어, JDBC 적재·빈 배치 스킵·타이밍 로그·배치 가드 중복 방지를 하나의 흐름으로 결합했다. Flink 커스텀 싱크나 Kafka Streams Processor API로도 유사하게 구현할 수 있지만, `foreachBatch` 패턴이 가장 직관적으로 표현된다.
- **선언적 배압 제어**: `maxOffsetsPerTrigger`로 트리거당 Kafka 소비량을 제한해 ClickHouse 과부하를 Spark 레벨에서 방어한다. Kafka Streams는 배압을 직접 구현해야 한다.
- **ClickHouse JDBC 생태계**: 이 프로젝트 개발 시점에 Flink ClickHouse 커넥터는 프로덕션 안정성이 낮았다. Spark JDBC는 드라이버·설정이 성숙하고 레퍼런스가 많다.
- **Python 생태계 통일**: 시뮬레이터(FastAPI), 공통 모듈, Spark 잡 모두 Python으로 언어를 통일했다. Kafka Streams는 Java 전용이다.

**솔직히 인정할 것**

현재 처리 규모(~1,000 EPS, 4초 트리거, 배치당 ~4,000행)에서는 Kafka Streams나 Flink로도 동일한 처리량을 낼 수 있다. Spark가 진가를 발휘하는 대규모 join, aggregation, ML pipeline은 현재 로직에 없다. Spark를 선택한 핵심 이유는 기술적 최적이 아니라 **PoC 환경에서 빠른 구현, 배치·스트리밍 단일화, foreachBatch 패턴의 표현력**이다. 스케일아웃이 필요한 시점에 Flink 전환을 고려할 것이다.

---

## 2. 분산처리가 실제로 효율적으로 되고 있나?

**핵심 답변**

> "Kafka 읽기 구간에서는 파티션 수(7개)만큼 병렬 task가 실행됩니다.  
> 다만 JDBC write 구간에서는 coalesce(3) 이후 커넥션 3개가 순차 INSERT를 하므로, write 단계에서는 분산 효과가 제한적입니다.  
> 현재 처리량과 트리거 주기 내에 처리가 완료되고 있어 병목은 없습니다."

**레이어별 실제 분산 현황**

| 구간 | 병렬성 | 비고 |
|---|---|---|
| Kafka 읽기 | 7 task (파티션 수) | auth 3, order 2, payment 1, error 1 |
| JSON 파싱·변환 | executor 병렬 실행 | shuffle 없음, CPU 경량 |
| coalesce(3) | 3 partition으로 축소 | 소형 파티션 파편화 방지 |
| JDBC write | 3 커넥션, batchsize=10000 순차 INSERT | 진정한 분산 write 아님 |

**설계상 의도적으로 분산을 포기한 부분**

- dedup 비활성화(`SPARK_FACT_DEDUP_KEYS=`): watermark 기반 `dropDuplicatesWithinWatermark`는 shuffle + 상태 관리 비용이 크다. 중복 발생 확률이 낮고, ClickHouse `ReplacingMergeTree`가 엔진 레벨에서 dedup을 담당하므로 비활성화했다.
- 집계 Spark 미적용: 윈도우 집계를 Spark에서 하면 watermark 지연(최소 수십 초)이 발생한다. ClickHouse Materialized View가 INSERT 즉시 집계를 갱신하므로 Spark에서 집계를 제거하고 지연을 줄였다.

---

## 3. dedup을 왜 Spark에서 비활성화하고 ClickHouse에서 처리하나?

**핵심 답변**

> "dedup을 Spark에서 ClickHouse로 이관한 것은 의도적인 레이어 분리 결정입니다.  
> Spark는 저지연 전달에 집중하고, 멱등성 보장은 ReplacingMergeTree의 엔진 레벨 dedup에 위임했습니다.  
> 중복 발생 확률이 낮은 상황에서 Spark watermark 기반 dedup은 지연과 상태 관리 비용만 추가합니다."

**Spark dedup을 활성화했을 때 비용**

| 항목 | 영향 |
|---|---|
| watermark 지연 | 설정값(예: 1시간)만큼 집계 결과 확정 지연 |
| shuffle 발생 | `dropDuplicatesWithinWatermark` → executor 간 네트워크 I/O + 디스크 spill |
| 상태 크기 | watermark 범위 내 모든 event_id를 메모리·체크포인트에 유지 |
| 실제 방어 효과 | 중복 발생 확률이 낮아 거의 작동하지 않음 |

**ClickHouse가 커버하는 방식**

```sql
ENGINE = ReplacingMergeTree(spark_processed_at)
ORDER BY (service, event_id)
```

같은 `(service, event_id)` 조합이 들어오면 `spark_processed_at`이 최신인 행만 남긴다. 파트 병합이 비동기라 즉각적인 dedup은 보장되지 않지만, 재시작·재처리처럼 실제로 중복이 생기는 케이스에서는 결국 수렴된다. 조회 시 `FINAL` 키워드로 강제 즉시 dedup도 가능하다.

---

## 4. Kafka 파티션 수와 Spark worker 코어를 어떻게 결정했나?

**핵심 답변**

> "파티션 수는 서비스별 예상 처리량에 비례해 설계했고, 코어 수는 트리거 내 처리 완료 여부로 검증했습니다.  
> 코어 수를 파티션 수에 정확히 맞추는 것보다 트리거 주기 내에 처리가 완료되는지가 더 중요한 지표입니다."

**파티션 설계 근거**

| 토픽 | 파티션 수 | 이유 |
|---|---|---|
| logs.auth | 3 | 가장 높은 트래픽, 병렬 소비 필요 |
| logs.order | 2 | 중간 트래픽 |
| logs.payment | 1 | 낮은 트래픽, 순서 보장 선호 |
| logs.error | 1 | 거의 데이터 없음 |

**코어 증설이 의미 없는 이유**

7 task / 5 core → 2 라운드 스케줄링 발생. worker-2를 core 2→3으로 올리면 6코어가 되지만 7 task > 6 core이므로 마지막 1 task는 여전히 대기한다. 트리거 내에 처리가 완료되고 있다면 코어 추가는 이득이 없고 메모리 경합만 증가한다.

**파티션 축소가 의미 없는 이유**

코어(5)에 맞춰 파티션을 줄이면 logs.auth의 프로듀서 처리량이 제한된다. Kafka 파티션은 한 번 줄이면 삭제 후 재생성만 가능하므로 오프셋이 초기화된다. 현재 구성에서 병목이 없으므로 조정 이유도 없다.

---

## 5. 집계를 왜 Spark가 아닌 ClickHouse Materialized View에서 하나?

**핵심 답변**

> "Spark에서 집계를 하면 watermark 지연이 필수입니다.  
> 10초 실시간 대시보드를 만들려면 `withWatermark('event_ts', '30 seconds')`를 설정해야 하고,  
> 그러면 집계 결과가 최소 30초 뒤에야 확정됩니다.  
> ClickHouse MV는 INSERT 즉시 동기 실행되어 지연이 없습니다."

**레이어별 역할 분리**

```
Spark    : Kafka 읽기 → 파싱 → 정규화 → 원본 적재 (지연 최소화)
ClickHouse MV : INSERT 트리거 → 1분/10초 집계 자동 갱신 (동기, 즉시)
Grafana  : 집계 테이블만 SELECT (fact_event 직접 쿼리 최소화)
```

Spark에 집계를 추가하면 stateful processing 비용(상태 직렬화, 체크포인트 I/O, shuffle)이 생기고, 트리거 주기마다만 집계가 갱신된다. ClickHouse MV는 INSERT와 동일한 트랜잭션 내에서 실행되므로 집계 갱신 지연이 없다.

---

## 6. at-least-once를 어떻게 effectively-once에 가깝게 만들었나?

**핵심 답변**

> "두 가지 레이어에서 중복을 방어합니다.  
> Spark의 `stream_batch_guard` 테이블이 재시작 시 이미 커밋된 배치를 skip하고,  
> ClickHouse `ReplacingMergeTree`가 파트 병합 시 event_id 기준으로 최신 행만 남깁니다."

**PoC에서 비활성화한 이유**

현재 `SPARK_RESET_CHECKPOINT_ON_START=true`, `SPARK_CLICKHOUSE_BATCH_GUARD_ENABLED=false`로 설정되어 있다. 재시작 시 항상 최신 오프셋부터 시작하는 PoC 운영 모드이므로, 배치 가드의 실질적인 보호 대상(장애 후 동일 배치 재처리)이 발생하지 않는다. 운영 환경 전환 시에는 체크포인트를 보존하고 배치 가드를 활성화해 effectively-once를 강화할 수 있다.

---

## 7. ClickHouse에 async_insert를 적용한 이유는?

**핵심 답변**

> "Spark JDBC는 coalesce(3) 이후 커넥션 3개로 나눠 소량 배치를 INSERT합니다.  
> 소량 INSERT가 빈번하면 ClickHouse 파트 생성이 과도해져 병합 부하가 커집니다.  
> async_insert는 소량 INSERT를 서버 내부 버퍼에서 묶어 한 번에 플러시해 파트 생성 빈도를 줄입니다."

**설정 선택 근거**

- `wait_for_async_insert=0`: JDBC `.save()` 반환을 flush 완료가 아닌 버퍼 수신 완료 시점으로 설정. Spark 입장에서 빠르게 반환되므로 throughput 우선 설계.
- `log_user`에만 적용: ingest 경로(`log_user`)와 읽기 경로(`grafana_user`)를 자원 격리. Grafana 쿼리 응답 지연에 영향 없음.
- 단점: flush 직전 재시작 시 버퍼 데이터 유실 위험. PoC에서는 수용 가능한 트레이드오프로 판단.

---

## 8. 이 프로젝트에서 배운 것과 실제 운영 환경에서 바꿀 것은?

**바꿀 것**

| 항목 | PoC 현재 | 운영 전환 시 |
|---|---|---|
| Kafka 브로커 수 | 1개 | 최소 3개 + 복제 |
| ClickHouse 구성 | 단일 노드 | Replica 구성 |
| 체크포인트 초기화 | 재시작마다 초기화 | 체크포인트 보존 |
| 배치 가드 | 비활성화 | 활성화 |
| Spark-ClickHouse 연동 | JDBC | clickhouse-spark-connector (네이티브 병렬 write) |
| 보존 기간 | 2시간 (디스크 절감) | 워크로드에 맞게 확대 |

**배운 것**

- 컴포넌트 간 자원 경합이 있는 단일 VM 환경에서는 각 컴포넌트의 역할을 명확히 분리하고 불필요한 연산을 제거하는 것이 코어/메모리 증설보다 효과적이다.
- 집계 책임을 Spark에서 ClickHouse MV로 이관한 것처럼, 각 시스템이 가장 잘하는 역할을 맡기는 레이어 설계가 전체 지연을 줄이는 핵심이었다.
- 설정값 하나(watermark, maxOffsets, trigger interval)가 E2E 지연 전체에 연쇄적으로 영향을 미치므로, 튜닝 전에 병목 지점을 명확히 파악하는 것이 중요하다.
