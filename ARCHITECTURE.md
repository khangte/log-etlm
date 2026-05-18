# Architecture

## 개요

대규모 로그 데이터의 수집·처리·모니터링을 검증하는 PoC 프로젝트.  
FastAPI 시뮬레이터가 Kafka에 이벤트를 발행하면, Spark Structured Streaming이 이를 소비해 ClickHouse에 적재하고, Grafana가 집계 테이블을 쿼리해 대시보드로 시각화한다.

> **운영 제약**: 단일 VM(vCPU 7) 환경. CPU 경합으로 초저지연 실시간은 비현실적이며, **약 10초 지연을 허용하는 near-real-time**을 목표로 설계되어 있다.

---

## 시스템 구성

```
┌─────────────────────────────────────────────────────────────────┐
│                        Docker Compose                           │
│                                                                 │
│  ┌──────────────┐      ┌──────────────┐     ┌───────────────┐  │
│  │  simulator   │─────▶│    kafka     │────▶│  spark-driver │  │
│  │  (FastAPI)   │      │  (KRaft)     │     │  (Streaming)  │  │
│  └──────────────┘      └──────────────┘     └───────┬───────┘  │
│                                                      │          │
│  ┌──────────────┐      ┌──────────────┐     ┌───────▼───────┐  │
│  │   grafana    │◀─────│  clickhouse  │◀────│  spark-batch  │  │
│  │  (dashboard) │      │   (OLAP)     │     │  (DIM 갱신)   │  │
│  └──────────────┘      └──────────────┘     └───────────────┘  │
│                                                                 │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │
│  │  kafka-ui    │  │    ch-ui     │  │      watchdog        │  │
│  └──────────────┘  └──────────────┘  └──────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

---

## 엔드투엔드 데이터 흐름

```
[log_simulator] ──produce──▶ [Kafka topics]
                                logs.auth
                                logs.order
                                logs.payment
                                logs.error
                                logs.dlq (DLQ 우회)

[spark-driver] ──subscribe──▶ [Kafka] ──foreachBatch──▶ [ClickHouse]
  parse_event()                                           analytics.fact_event
  validate_event()                                        analytics.fact_event_dlq
  normalize_event()
  dedup (watermark 또는 foreachBatch)

[ClickHouse] ──Materialized View──▶ 집계 테이블 (1m / 10s)

[Grafana] ──SELECT──▶ 집계 테이블 ──▶ 대시보드 패널

[spark-batch] ──(수동/크론)──▶ [ClickHouse] analytics.dim_*
```

---

## 컴포넌트별 상세

### 1. log_simulator

**위치**: `log_simulator/`  
**기동**: `uvicorn log_simulator.main:app`  
**역할**: 서비스별 이벤트 로그를 생성해 Kafka에 발행한다.

#### 내부 파이프라인

```
profiles.yml + routes.yml
        │
        ▼
SimulatorEngine.start()
        │
        ├─ build_simulators()         # 서비스별 시뮬레이터 인스턴스 생성 (REGISTRY)
        ├─ allocate_service_eps()     # total_eps × share → 서비스별 EPS 분배
        │
        ▼
assemble_pipeline()
        │
        ├─ [ServiceLoop Tasks] × loops_per_service 개
        │     └─ run_simulator_loop() # 토큰 버킷 기반 배치 생성 → publish_queue
        │
        └─ [PublisherWorker Tasks] × PUBLISHER_WORKERS 개
              └─ collect_batch() → KafkaProducerClient.publish_batch()
```

#### 핵심 설계

| 항목 | 내용 |
|---|---|
| **EPS 제어** | 토큰 버킷: `carry += eps × dt_actual` → `floor(carry)` = batch_size. 실제 경과시간 기반으로 sleep 오차를 자동 보정 |
| **time_weights** | `profiles.yml`의 시간대(KST)별 multiplier로 트래픽 패턴 시뮬레이션 |
| **2단계 backpressure** | soft throttle(큐 85%↑, scale 단계 감소) + hard throttle(90%↑, drain 대기) |
| **loops_per_service** | 서비스당 N개 루프를 병렬 실행해 asyncio 단일 스레드 한계 우회 |
| **event_id canonical seed** | allowlist 고정 필드(service, domain, event_name, ts_ms 등)를 BLAKE2b(16B)로 해싱. 스키마 확장 시 기존 event_id 불변 보장 |
| **event_mode** | `domain`(기본) / `http` / `all`. `domain`만 발행 시 Spark dedup 상태 부하 감소 |
| **replicate_error** | status_code ≥ 500 또는 result == "fail"인 이벤트를 `logs.error` 토픽에도 자동 복제 |

#### 이벤트 구조 (JSON)

```json
{
  "event_id":       "evt_v1_<blake2b_hex>",
  "event_name":     "login_success | http_request_completed | ...",
  "domain":         "auth | order | payment | http",
  "ts_ms":          1700000000000,
  "service":        "auth | order | payment",
  "request_id":     "req_<12hex>",
  "method":         "POST",
  "route_template": "/api/v1/auth/login",
  "status_code":    200,
  "duration_ms":    42,
  "result":         "success | fail",
  "user_id":        "...",
  "order_id":       "...",
  "payment_id":     "...",
  "reason_code":    "...",
  "amount":         null
}
```

---

### 2. Kafka

**이미지**: `confluentinc/cp-kafka:7.6.1` (KRaft 모드, 단일 노드)  
**역할**: 생산자(simulator)와 소비자(Spark) 사이의 로그 버퍼.

| 토픽 | 용도 |
|---|---|
| `logs.auth` | 인증 서비스 이벤트 |
| `logs.order` | 주문 서비스 이벤트 |
| `logs.payment` | 결제 서비스 이벤트 |
| `logs.error` | 에러 이벤트 (replicate_error 복제본) |
| `logs.dlq` | Spark DLQ 우회 메시지 |
| `logs.unknown` | 미분류 이벤트 |

**주요 설정**

```
KAFKA_LOG_RETENTION_HOURS=2        # 로컬 디스크 보호 (단기 보관)
KAFKA_MESSAGE_MAX_BYTES=1048576    # 1 MiB 상한
KAFKA_HEAP_OPTS=-Xms1g -Xmx1536m
```

---

### 3. spark-driver (Structured Streaming)

**위치**: `spark_job/`  
**기동**: `python -m spark_job.main`  
**역할**: Kafka에서 이벤트를 소비해 ClickHouse에 near-real-time으로 적재한다.

#### 처리 파이프라인

```
Kafka readStream
    │
    ▼
parse_event()
  ├─ CAST(value AS STRING) → raw_json
  ├─ from_json(raw_json, log_value_schema) → json 구조체
  ├─ spark_ingest_ts = current_timestamp()  ← Spark 파싱 시각
  └─ kafka_ts, topic, partition, offset 보존
    │
    ▼
validate_event()
  ├─ json IS NOT NULL  → good_df
  └─ json IS NULL      → bad_df  (error_type = "json_parse_failed")
    │
    ▼ (good_df)
normalize_event()
  ├─ unified_ts_ms = COALESCE(ts_ms, timestamp_ms, kafka_ts×1000)
  ├─ created_ts = to_timestamp(unified_ts_ms / 1000)
  ├─ event_ts = created_ts           ← dedup/watermark 기준
  ├─ ingest_ts = kafka_ts            ← Kafka 도착 시각
  ├─ ingest_ms = ingest_ts - event_ts
  └─ 레거시 필드 coalesce (path, event, timestamp_ms)
    │
    ▼
ClickHouseFactWriter.write_fact_event_stream()
  ├─ [선택] withWatermark("event_ts", "1 hour")
  │         + dropDuplicatesWithinWatermark(["event_id"])  ← 상태 기반 dedup
  │
  └─ foreachBatch(_foreach)
       ├─ coalesce(SPARK_CLICKHOUSE_WRITE_PARTITIONS)
       ├─ [선택] dropDuplicates(dedup_keys)   ← 배치 dedup (상태 dedup 미사용 시)
       ├─ processed_ts = current_timestamp()  ← sink 직전 재계산
       ├─ process_ms = processed_ts - spark_ingest_ts
       └─ write_to_clickhouse()
            ├─ 배치 가드 체크 (stream_batch_guard)
            ├─ JDBC write → analytics.fact_event
            └─ 배치 가드 기록
```

#### DLQ 흐름 (`SPARK_ENABLE_DLQ_STREAM=true` 시)

```
bad_df
  └─ KafkaDlqWriter → logs.dlq 토픽 재발행
       └─ KafkaStreamBuilder → logs.dlq 소비
            └─ build_dlq_stream_df() → parse_dlq()
                 └─ ClickHouseDlqWriter → analytics.fact_event_dlq
```

#### 타임스탬프 컬럼 흐름

| 컬럼 | 의미 | 생성 위치 |
|---|---|---|
| `created_ts` | 이벤트 생성 시각 (ts_ms 기반) | normalize_event |
| `event_ts` | dedup·watermark 기준 (= created_ts) | normalize_event |
| `ingest_ts` | Kafka 도착 시각 (= kafka_ts) | normalize_event |
| `kafka_ts` | Kafka timestamp | parse_event |
| `spark_ingest_ts` | Spark 파싱 시각 | parse_event |
| `processed_ts` | foreachBatch 직전 재계산 | writer_base |
| `stored_ts` | ClickHouse DEFAULT now64(3) | ClickHouse DDL |

#### 주요 환경변수

| 변수 | 기본값 | 설명 |
|---|---|---|
| `SPARK_FACT_TRIGGER_INTERVAL` | `4 seconds` | 스트리밍 트리거 주기 |
| `SPARK_MAX_OFFSETS_PER_TRIGGER` | 자동 계산 | 배치당 최대 오프셋. 미설정 시 `target_eps × trigger_sec × safety`로 계산 |
| `SPARK_MAX_OFFSETS_CAP` | 30000 | maxOffsets 하드 상한 |
| `SPARK_FACT_DEDUP_KEYS` | `event_id` | dedup 기준 컬럼 |
| `SPARK_FACT_DEDUP_WATERMARK` | `1 hour` | 상태 기반 dedup 워터마크 |
| `SPARK_CLICKHOUSE_BATCH_GUARD_ENABLED` | `true` | 배치 가드 on/off |
| `SPARK_RESET_CHECKPOINT_ON_START` | `false` | 시작 시 체크포인트 초기화 |
| `SPARK_ENABLE_DLQ_STREAM` | `false` | DLQ 스트림 활성화 |
| `SPARK_SKIP_EMPTY_BATCH` | `false` | 빈 배치 skip |
| `SPARK_CLICKHOUSE_WRITE_PARTITIONS` | `3` | ClickHouse 쓰기 파티션 수 |

---

### 4. ClickHouse

**이미지**: `clickhouse/clickhouse-server:25.8`  
**역할**: 로그 이벤트 OLAP 저장소. Materialized View로 집계를 자동 갱신한다.

#### 테이블 구조

```
analytics
├── fact_event               MergeTree,  TTL 1일  (원본 이벤트)
├── fact_event_dlq           MergeTree,  TTL 7일  (파싱 실패)
├── stream_batch_guard       MergeTree,  TTL 30일 (배치 멱등성)
│
├── fact_event_agg_1m        AggregatingMergeTree, TTL 2일  (1분 EPS·에러율)
├── fact_event_latency_1m    AggregatingMergeTree, TTL 2일  (1분 지연 p95)
├── fact_event_latency_service_1m  AggMT,  TTL 2일  (서비스별 단계 지연)
├── fact_event_freshness_1m  AggregatingMergeTree, TTL 2일  (데이터 신선도)
├── fact_event_created_stored_1m  SummingMT, TTL 2일  (생성·적재 비율)
├── fact_event_lag_1m        SummingMergeTree,     TTL 2일  (event→ingest 편차)
├── fact_event_dlq_agg_1m    SummingMergeTree,     TTL 8일  (DLQ 에러 집계)
│
├── fact_event_agg_10s       AggregatingMergeTree, TTL 1일  (10초 실시간 EPS)
├── fact_event_latency_10s   AggregatingMergeTree, TTL 1일  (10초 실시간 지연)
├── fact_event_latency_stage_10s  AggMT, TTL 1일  (10초 단계 지연)
│
└── dim_service / dim_status_code / dim_date / dim_time / dim_user   (배치 갱신)
```

#### Materialized View 동작

`fact_event` INSERT 시 각 MV가 자동으로 집계 테이블에 INSERT:

```
fact_event INSERT
    ├──▶ mv_fact_event_agg_1m              → countState(EPS, 에러율)
    ├──▶ mv_fact_event_latency_1m          → quantileTDigestState(지연 p95)
    ├──▶ mv_fact_event_latency_service_1m  → 단계별 p95 (producer→kafka→spark→stored)
    ├──▶ mv_fact_event_freshness_1m        → maxState(ingest_ts)
    ├──▶ mv_fact_event_created_stored_1m   → created/stored 버킷 카운트
    ├──▶ mv_fact_event_lag_1m              → event_ts→ingest_ts 편차 누적
    ├──▶ mv_fact_event_agg_10s             → 10초 EPS (부하 시 DETACH 가능)
    ├──▶ mv_fact_event_latency_10s         → 10초 지연
    └──▶ mv_fact_event_latency_stage_10s   → 10초 단계 지연
```

#### 사용자 권한

| 사용자 | 역할 | 특이사항 |
|---|---|---|
| `log_user` | Spark JDBC 쓰기 | `log_async_profile` 적용 (async_insert=1, wait=0, 6GB 메모리) |
| `grafana_user` | SELECT 전용 | max_memory=1GB, max_threads=4 (ingest 격리) |

#### 배치 가드 (`stream_batch_guard`)

```sql
(stream_name, target_table, batch_id) 기준 중복 배치 skip
```

Spark `foreachBatch`의 exactly-once를 보완한다. 가드 테이블 미존재 시 자동 비활성화(기동 경고 출력).

---

### 5. spark-batch (DIM 배치)

**기동**: `docker compose run --rm spark-batch` 또는 크론  
**역할**: `fact_event`에서 최근 N일 데이터를 읽어 5개 차원 테이블을 갱신한다.

```
fact_event (최근 DIM_BATCH_LOOKBACK_DAYS일)
    ├──▶ parse_dim_date()        → dim_date
    ├──▶ parse_dim_time()        → dim_time
    ├──▶ parse_dim_service()     → dim_service  (service_map CSV 선택 적용)
    ├──▶ parse_dim_status_code() → dim_status_code
    └──▶ parse_dim_user()        → dim_user
```

> DIM 배치는 자동 실행되지 않는다. 수동 실행 또는 크론(`scripts/dim_spark_batch.sh`) 필요.

---

### 6. Grafana

**이미지**: `grafana/grafana:10.4.2`  
**플러그인**: `grafana-clickhouse-datasource`  
**역할**: ClickHouse 집계 테이블을 쿼리해 운영 지표를 시각화한다.

| 대시보드 | 집계 소스 | 새로고침 | 주요 패널 |
|---|---|---|---|
| `ops_monitoring.json` | 1분 집계 | 30초 | EPS, 에러율, E2E 지연 p95, Freshness, 생성·적재 비율, 단계별 지연 |
| `realtime.json` | 10초 집계 | 10초 | 실시간 EPS, 지연 |
| `dim_overview.json` | dim 테이블 | 비활성 | 서비스·상태코드 현황 |

> 프로비저닝 파일(`provisioning/`)이 source-of-truth. `allowUiUpdates: false`로 UI 수정이 파일을 덮지 않도록 설정되어 있다.

---

### 7. watchdog

**위치**: `infra/monitor/main.py`  
**역할**: asyncio 기반 경량 모니터링. Prometheus 없이 최소 방어선 구성.

```
asyncio.gather()
    ├─ watch_events()           # docker events → die/unhealthy 감지
    ├─ watch_logs()             # docker logs -f → 키워드 패턴 매칭
    │    kafka:        OutOfMemoryError, Fatal error
    │    spark-driver: OOM, StreamingQueryException, Job aborted, Code:241
    │    clickhouse:   Code:241, Memory limit exceeded, DB::Exception
    │    grafana:      level=error, panic:
    ├─ check_health_loop()      # docker inspect health (30초 주기)
    ├─ spark_rest_probe()       # localhost:4040/api/v1/applications (30초)
    ├─ grafana_health_probe()   # localhost:3000/api/health (30초)
    └─ clickhouse_stage_probe() # ClickHouse 집계 쿼리 (10분 주기)
         EPS, 에러율, 단계별 p95, Freshness, DLQ 비율 임계값 비교
```

**알림 제어**

| 변수 | 기본값 | 설명 |
|---|---|---|
| `ALERT_BREACH_GRACE_SEC` | 300s | 임계값 초과가 이 시간 이상 지속돼야 알림 발송 (스파이크 필터) |
| `ALERT_COOLDOWN_SEC` | 300s | 동일 키 중복 알림 방지 쿨다운 |
| `ALERT_WEBHOOK_URL` | 없음 | Slack Webhook URL (없으면 stdout만) |

---

## 지표 해석

| 지표 | 계산식 | 의미 |
|---|---|---|
| **ingest 지연** | `ingest_ts - event_ts` | Producer 생성 → Kafka 도착 |
| **kafka→spark 지연** | `spark_ingest_ts - ingest_ts` | Kafka → Spark 파싱 |
| **spark processing** | `processed_ts - spark_ingest_ts` | Spark 파싱 → ClickHouse 전송 직전 |
| **sink 지연** | `stored_ts - processed_ts` | Spark → ClickHouse 적재 |
| **E2E (운영 기준)** | `stored_ts - ingest_ts` | Kafka 도착 → ClickHouse 적재 |
| **생성·적재 비율** | `stored_cnt / created_cnt` (1분 버킷) | 지연 클 때 0%로 보일 수 있음 |

---

## 체크포인트 및 복구

체크포인트 경로: `/data/log-etlm/spark_checkpoints/fact_event`

```bash
# 체크포인트가 존재하면 SPARK_STARTING_OFFSETS는 무시되고 기존 오프셋에서 재개됨

# 체크포인트를 리셋하려면 (재시작 시 백업 후 초기화)
SPARK_RESET_CHECKPOINT_ON_START=true  # docker-compose.yml에 설정
```

---

## 디렉터리 구조

```
log-etlm/
├── log_simulator/          # FastAPI 시뮬레이터
│   ├── simulator/          # 서비스별 이벤트 생성 (BaseServiceSimulator)
│   ├── publisher/          # Kafka 퍼블리셔 워커
│   ├── producer/           # confluent_kafka 래퍼
│   └── config/             # profiles.yml, routes.yml, timeband
│
├── spark_job/              # Spark 스트리밍·배치 잡
│   ├── fact/               # 팩트 이벤트 파싱·정규화·적재
│   ├── dlq/                # DLQ 파싱·적재
│   ├── dimension/          # DIM 배치 잡
│   └── clickhouse/         # ClickHouse JDBC 싱크 공통 (배치 가드 포함)
│
├── infra/
│   ├── clickhouse/
│   │   ├── sql/            # DDL (00_database, 10_fact, 20_aggregates, 21_mv, 30_dim) + 90_grants.sh (init)
│   │   ├── config.d/       # listen_host, async insert 로그
│   │   └── users.d/        # log_user async 프로파일
│   ├── grafana/
│   │   ├── dashboards/     # ops_monitoring.json, realtime.json, dim_overview.json
│   │   └── provisioning/   # datasources/clickhouse.yaml, dashboards/default.yaml
│   └── monitor/
│       ├── main.py             # 진입점 (asyncio.gather)
│       └── watchdog/           # 모니터링 패키지
│           ├── config.py       # 환경변수 상수, LOG_PATTERNS
│           ├── alert.py        # Webhook/stdout 알림, cooldown/grace
│           ├── docker_probes.py  # docker events/logs/health
│           ├── service_probes.py # Spark REST, Grafana health
│           └── clickhouse_probe.py  # ClickHouse 집계 임계값 점검
│
├── scripts/                # 운영 유틸 (Spark 프로파일 전환, DIM 배치, 진단)
├── config/env/             # low·mid·high.env (Spark 튜닝 프로파일)
├── docs/
│   ├── clickhouse_aggregate_guide.md
│   └── oom_prevention_checklist.md
├── ARCHITECTURE.md         # 이 문서
└── docker-compose.yml
```

---

## 관련 문서

- [ClickHouse 집계/권한/백필 가이드](docs/clickhouse_aggregate_guide.md) — 집계 테이블 추가·백필 절차
- [OOM 방지 체크리스트](docs/oom_prevention_checklist.md) — 컴포넌트별 OOM 대응 절차
- [README](README.md) — 실행 방법, 튜닝 포인트, 유틸 스크립트 목록
