# 안정성 설정 결정 근거

이 문서는 파이프라인 안정화 과정에서 조정된 핵심 설정값의 트레이드오프와 유지 근거, 그리고 중복 이벤트 처리 전략을 기록한다.

---

## 배경

단일 VM(Docker Compose) 환경에서 Simulator → Kafka → Spark → ClickHouse 파이프라인이 간헐적으로 불안정했다.  
아래 세 값을 조정한 이후 30분 이상 안정적인 운영이 확인되었다.

---

## 1. `LOG_BATCH_SIZE`: 2000 → 500

### 역할
Simulator가 Kafka producer에 한 번에 넘기는 이벤트 묶음 크기.

### 트레이드오프

| | 설명 |
|---|---|
| 잃는 것 | producer 호출 횟수 증가 → 메시지당 오버헤드 소폭 상승 |
| 얻는 것 | publisher 큐 점유가 완만하게 상승 → 백프레셔 임계값(`QUEUE_THROTTLE_RATIO=0.9`) 침범 빈도 감소 |

### 유지 근거
`TARGET_INTERVAL_SEC=0.50`, `PUBLISHER_WORKERS=3` 기준 초당 최대 처리량은  
`3 × 500 / 0.5 = 3,000 events/s`이며 `WORKER_BATCH_SIZE=600`이 상한이다.  
throughput 한계에 도달하지 않는 범위 안에서 큐 부하 패턴을 평탄화한 것이므로 처리량 손실이 없다.

---

## 2. `SPARK_SKIP_EMPTY_BATCH`: false → true

### 역할
Kafka에서 읽은 레코드가 0개인 마이크로배치를 ClickHouse write 없이 즉시 종료할지 여부.

### 트레이드오프

| | 설명 |
|---|---|
| 잃는 것 | "빈 배치도 기록해야 하는" 감사(audit) 요건이 있을 경우 누락 |
| 얻는 것 | Idle 구간에 JDBC 커넥션·`BATCH_GUARD` 테이블 체크가 완전히 제거됨 |

### 유지 근거
이 파이프라인의 목적은 이벤트 적재이며, 빈 배치는 적재할 데이터가 없다는 뜻이다.  
`SPARK_CLICKHOUSE_BATCH_GUARD_ENABLED=true`가 켜져 있으므로 실제 데이터가 있는 배치의 중복·누락 보호는 그대로 동작한다.  
감사 목적의 빈 배치 기록은 이 파이프라인의 요건이 아니다.

---

## 3. `SPARK_FACT_DEDUP_KEYS`: `event_id` → (비활성화)

### 역할
Spark Structured Streaming이 watermark 윈도우 안에서 `event_id` 기준으로 중복 이벤트를 제거하던 stateful 연산.

### 트레이드오프

| | 설명 |
|---|---|
| 잃는 것 | Spark 레벨 중복 제거 없음. 동일 `event_id`가 Kafka에 두 번 들어오면 두 번 적재될 수 있음 |
| 얻는 것 | Stateful 처리 제거 → executor GC 압박 해소, 체크포인트 상태 크기 감소, 마이크로배치 처리 시간 예측 가능 |

### 유지 근거
중복은 아래 다른 레이어에서 이미 처리된다(다음 절 참조).

---

## 중복 이벤트 처리 전략

Spark dedup을 끄더라도 파이프라인 전반에 걸쳐 세 레이어가 중복을 억제한다.

### 레이어 1 — Simulator (발행 측)

```
PRODUCER_ENABLE_IDEMPOTENCE=false
PRODUCER_ACKS=0
```

`acks=0`은 at-most-once 설정이다. producer가 재전송을 시도하지 않으므로, 네트워크 단절이 아닌 이상 동일 이벤트가 Kafka에 두 번 들어갈 경로 자체가 없다.

`event_id`는 UUID가 아닌 콘텐츠 해시(`blake2b-128`)다. seed에는 `request_id`(per-request), `ts_ms`(ms 단위), `service`, `event_name` 등 다수 필드가 포함된다.  
`request_id`는 `uuid.uuid4().hex`(122-bit 실질 엔트로피)로 생성되므로, 서로 다른 요청이 동일한 `event_id`를 가질 확률은 구조적으로 무시 가능한 수준이다.

→ **중복 발행 가능성이 구조적으로 낮음.**

### 레이어 2 — ClickHouse (저장 측)

```sql
-- 예: event_log 테이블
ENGINE = ReplacingMergeTree(...)
ORDER BY (event_id, ...)
```

`ReplacingMergeTree`는 백그라운드 merge 시 동일 정렬 키(`event_id`)를 가진 행 중 최신 버전만 남긴다.  
→ **적재된 중복은 merge 완료 후 자동 제거됨.**  
단, merge 전 짧은 시간 동안 집계 쿼리에 중복이 보일 수 있으며, 정확한 집계가 필요하면 `FINAL` 키워드를 사용한다.

### 레이어 3 — Spark `BATCH_GUARD` (쓰기 측)

```
SPARK_CLICKHOUSE_BATCH_GUARD_ENABLED=true
SPARK_CLICKHOUSE_BATCH_GUARD_TABLE=analytics.stream_batch_guard
```

동일 배치 ID가 이미 기록된 경우 해당 배치의 ClickHouse write를 건너뛴다.  
Spark 재시작·체크포인트 재처리 시 동일 배치가 두 번 실행되는 경우를 방어한다.  
→ **Spark 재처리로 인한 배치 단위 중복을 방지.**

### 레이어별 역할 요약

| 레이어 | 대상 | 방식 |
|---|---|---|
| Simulator `acks=0` | 발행 단계 재전송 | 구조적 차단 |
| ClickHouse `ReplacingMergeTree` | 저장된 중복 행 | 백그라운드 merge로 자동 제거 |
| Spark `BATCH_GUARD` | 배치 재처리 중복 | 배치 ID 기반 멱등성 보장 |

---

## 프로덕션 전환 시 재검토 항목

현재 설정은 **PoC/모니터링 목적**에서 안정성을 우선한 선택이다.  
정확한 트랜잭션 처리가 필요한 환경으로 전환할 경우 아래를 재검토한다.

| 항목 | 현재 값 | 재검토 방향 |
|---|---|---|
| `SPARK_FACT_DEDUP_KEYS` | 비활성화 | `event_id` 재활성화, watermark 범위 설정 |
| `PRODUCER_ACKS` | `0` (at-most-once) | `1` 또는 `all` (at-least-once) |
| `PRODUCER_ENABLE_IDEMPOTENCE` | `false` | `true` + `acks=all` (exactly-once 근접) |

`LOG_BATCH_SIZE=500`과 `SPARK_SKIP_EMPTY_BATCH=true`는 어떤 환경에서도 현재 값이 합리적이다.
