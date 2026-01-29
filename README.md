# Log ETL & Monitoring PoC Project

## 개요

![ops대시보드](images/화면%20캡처%202026-01-29%20151906.jpg)
![dim대시보드](images/화면%20캡처%202026-01-28%20154256.jpg)

대규모 로그 데이터에 대한 **수집, 처리, 모니터링**을 목표로 하는 **PoC(Proof of Concept) 프로젝트**입니다.

- 대용량 이벤트 로그를 실시간으로 수집‧가공‧시각화하는 파이프라인을 검증했습니다.
- FastAPI 기반 시뮬레이터가 Kafka 로그 토픽에 다양한 서비스 패턴을 발행하면, Spark Structured Streaming 잡이 이를 ClickHouse 분석 테이블로 적재하고 Grafana 대시보드로 노출합니다. 각 컴포넌트는 Docker Compose로 손쉽게 기동할 수 있으며, ClickHouse 초기 스키마와 Grafana 프로비저닝도 자동화되어 있어 부팅 직후부터 엔드투엔드 흐름을 검증할 수 있습니다.


## 목표

- 대규모 로그 스트림의 ~~real-time(실시간)~~ **near-real-time(준실시간)** 제약 하 안정 처리 가능성 검증
- FastAPI → Kafka → Spark → ClickHouse → Grafana 엔드투엔드 파이프라인의 **지연/처리량 목표** 충족 여부 확인
- 각 단계별 병목 지점 식별 및 개선 방안 도출
- 추가: Slack 연동 Watchdog과 Grafana 대시보드 기반 최소 운영 감시 체계 구성 및 실시간 알림/가시성 확보 가능성 검증


## 실험 환경 / 제약 및 결정(SLA)

- **환경** : VirtualBox Ubuntu 단일 VM에서 Simulator, Kafka, Spark(Structured Streaming), ClickHouse, Grafana를 Docker Compose로 동시 구동
- **리소스 변경 과정**:
  - vCPU 4로 시작 → Spark 처리(워커/코어) 병목 확인 후 증설
  - vCPU 6까지 증설해도 CPU 포화가 지속(컴포넌트 간 CPU 경합)
  - vCPU 8은 VM 강제 종료 문제로 운영 불가 
  - **vCPU 7도 포화 상태이지만 그나마 안정 구동 확인**
  - **관찰 결과**: 단일 VM에서 여러 컴포넌트가 CPU를 경쟁적으로 점유하여 “초저지연 실시간(수 초)” 목표는 비현실적임을 확인
  - **결정(SLA 재정의)**: 안정적인 지속 처리를 우선하여 **약 10초 수준의 지연을 허용하는 near-real-time** 목표로 조정  


## 기술 스택

| 아이콘 | 설명 |
| --- | --- |
| <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white"> | Python 3.10: 시뮬레이터, Watchdog 스크립트 등 보조 유틸 |
| <img src="https://img.shields.io/badge/FastAPI-009688?style=for-the-badge&logo=fastapi&logoColor=white"> | FastAPI: log_simulator 시뮬레이터 및 API 엔드포인트 |
| <img src="https://img.shields.io/badge/ApacheKafka-231F20?style=for-the-badge&logo=apachekafka&logoColor=white"> | Apache Kafka + Kafka UI: 로그 수집 버퍼와 모니터링 UI |
| <img src="https://img.shields.io/badge/ApacheSpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white"> | Apache Spark 4.0 Structured Streaming: Kafka → ClickHouse 실시간 적재 |
| <img src="https://img.shields.io/badge/ClickHouse-FFCC01?style=for-the-badge&logo=clickhouse&logoColor=white"> | ClickHouse: OLAP 테이블 로그 저장 |
| <img src="https://img.shields.io/badge/Grafana-F46800?style=for-the-badge&logo=grafana&logoColor=white"> | Grafana: ClickHouse 데이터 소스 기반 대시보드 시각화 |
| <img src="https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white"> | Docker / Docker Compose: 전체 개발 환경 오케스트레이션 |
| <img src="https://img.shields.io/badge/Ubuntu-E95420?style=for-the-badge&logo=ubuntu&logoColor=white"> | Linux (Ubuntu 기반): VM 환경 및 파일 시스템 레이아웃 |
| <img src="https://img.shields.io/badge/Slack-4A154B?style=for-the-badge&logo=slack&logoColor=white"> | Slack Webhook: Watchdog 알림 채널 연동 |


## 시스템 아키텍처

![시스템아키텍처](images/SystemArchitecture.png)

1. **로그 생성/수집**
   - `log_simulator/engine.py`가 `log_simulator/config/profiles.yml`, `log_simulator/config/routes.yml`을 읽어 서비스별 시뮬레이터/퍼블리셔 파이프라인을 구성.
   - FastAPI 앱(`log_simulator/main.py`)의 lifespan에서 엔진을 시작/중지하며 로그를 생성.
2. **로그 브로커/버퍼링**
   - Kafka 단일 노드가 `logs.auth`, `logs.order`, `logs.payment`, `logs.dlq`, `logs.error`, `logs.unknown` 토픽에서 생산자와 소비자 사이 메시지 큐 역할 수행.
   - Kafka UI를 통한 토픽/파티션 상태와 소비량 확인, 필요 시 수동 토픽 관리(생성/삭제) 수행.
3. **로그 실시간 처리**
   - `spark_job/main.py`가 `spark_job/stream_ingest.py`를 실행하고, fact/DLQ 스트림이 토픽을 분리 구독해 정규화/적재를 수행.
   - fact 토픽 목록은 `SPARK_FACT_TOPICS`, DLQ 토픽은 `SPARK_DLQ_TOPIC`으로 설정.
   - DLQ 스트리밍을 끄려면 `SPARK_ENABLE_DLQ_STREAM=false`.
   - `SPARK_PROGRESS_LOG_PATH`에 StreamingQuery 진행 로그(JSON lines)를 기록(기본 경로: `/data/log-etlm/spark-events/spark_progress.log`).
   - `SPARK_BATCH_TIMING_LOG_PATH`로 배치 타이밍 로그를 남길 수 있다.
   - Spark 스트림의 `/data/log-etlm/spark_checkpoints` 체크포인트 활용, 장애 복구 시점 유지.
   - 체크포인트가 있으면 `SPARK_STARTING_OFFSETS=latest` 설정은 무시되고 기존 오프셋에서 재개된다.
4. **로그 저장**
   - `spark_job/fact/writers/fact_writer.py`, `spark_job/dlq/writers/dlq_writer.py`가 ClickHouse `analytics.fact_event`, `analytics.fact_event_dlq` 테이블에 스트리밍 적재.
   - 초기 스키마는 `infra/clickhouse/sql/*.sql`로 자동 생성, `/data/log-etlm/clickhouse` 볼륨 영속화.
5. **로그 시각화 및 모니터링**
   - Grafana는 프로비저닝된 ClickHouse 데이터 소스로 EPS, 오류율, 상태 코드 분포 시각화.
   - 대시보드 JSON:
     - `infra/grafana/dashboards/ops_monitoring.json` (운영/1m 집계)
     - `infra/grafana/dashboards/realtime.json` (실시간/10s 집계)
     - `infra/grafana/dashboards/dim_overview.json` (DIM)
   - 실시간 대시보드는 10초 집계 테이블을 사용한다. 
     - 부하가 크면 **10s MV를 DETACH해서 비활성화**할 수 있다.
   - 기본 refresh: ops 2m / realtime 30s / dim 비활성화(빈 문자열).
   - 운영 대시보드는 Freshness, Kafka→Spark ingest 지연, Spark 처리/ClickHouse INSERT/Grafana 쿼리 p95, **생성 대비 적재 비율(1m)** 등의 운영 지표가 포함된다.
   - `infra/monitor/docker_watchdog.py`는 Kafka/Spark/ClickHouse/Grafana 컨테이너 이벤트와 로그를 감시해 OOM, StreamingQueryException, health 변화 등을 Slack Webhook/CLI로 통지한다.
     - `ALERT_BREACH_GRACE_SEC`로 지연 스파이크가 일정 시간 이상 지속될 때만 알림을 보낼 수 있다.


## 실행 방법

```bash
# 0. 사전 준비 단계
# - Docker / Docker Compose 설치
# - VM 환경: /data 파티션 마운트 및 디렉터리 생성, rw 권한 부여
#     sudo mkdir -p /data/log-etlm/kafka-logs /data/log-etlm/kafka-meta \
#                  /data/log-etlm/spark_checkpoints /data/log-etlm/spark-events \
#                  /data/log-etlm/clickhouse /data/log-etlm/clickhouse-logs \
#                  /data/log-etlm/grafana
#     sudo chown -R $USER:$USER /data/log-etlm
# - 방화벽/보안 그룹에서 29092(Kafka), 4040(Spark UI), 3000(Grafana) 허용
#   (선택) 8000(simulator), 8080(kafka-ui), 8081/8082(Spark master UI), 5521(ch-ui)
# - ClickHouse(8123/9000)는 기본적으로 localhost 바인딩(외부 접근 필요 시 docker-compose.yml 포트 수정)

# 1. Kafka + Kafka UI 우선 기동
docker compose up -d kafka kafka-ui

# 1-1. 도메인별 토픽 파티션 관리(필요할 때만)
# 토픽 파티션 생성/증설은 아래 "Kafka 토픽 파티션 분배(도메인 기준)" 섹션 참고

# 2. ClickHouse 기동
docker compose up -d clickhouse ch-ui

# 3. Spark 기동
docker compose up -d spark-master spark-worker-1 spark-worker-2

# 3-1. Spark-driver 기동 (Kafka/ClickHouse healthcheck 이후 권장)
docker compose up -d spark-driver

# 4. Grafana 기동
docker compose up -d grafana
# - grafana-clickhouse-datasource 플러그인 필요(온라인이면 GF_INSTALL_PLUGINS 사용)

# 5. 로그 시뮬레이터 기동 (Spark-driver 정상 기동 후)
docker compose up -d simulator

# 6. 상태 점검
docker compose ps
curl http://localhost:8000/ping                 # log_simulator FastAPI
curl http://localhost:4040/api/v1/applications  # Spark UI REST

# 7. (선택) CLI 모니터링
python infra/monitor/docker_watchdog.py

# 8. (선택) Spark 프로파일 자동 전환 크론 등록
#   - ClickHouse 지연 p95 기반 Spark 프로파일 주기 조정
#   - 로그 저장 위치: logs/autoswitch.log
crontab -e
*/10 * * * * /home/kang/log-etlm/scripts/autoswitch_spark_env.sh >> /home/kang/log-etlm/logs/autoswitch.log 2>&1
```


### DIM 배치(차원 테이블 갱신)

DIM 대시보드는 `analytics.dim_*` 테이블을 사용하며, **DIM 배치는 자동 실행되지 않습니다.**
필요할 때 수동 실행하거나 크론으로 스케줄링합니다.

- 대상 테이블: `dim_date`, `dim_time`, `dim_service`, `dim_status_code`, `dim_user`
- 입력 소스: `analytics.fact_event`
- 데이터 범위: 최근 N일 (`DIM_BATCH_LOOKBACK_DAYS`, 기본 1일)
- 기준 시간: `ingest_ts` (dim_date, dim_time 생성 기준)

#### 1. Spark 배치 컨테이너 실행 (권장)

```bash
# 직접 실행
docker compose run --rm spark-batch

# 중복 실행 방지(flock) 포함 스크립트
bash scripts/dim_spark_batch.sh
```

**옵션 환경 변수**
- `DIM_BATCH_LOOKBACK_DAYS`: fact_event 조회 범위(일)
- `DIM_SERVICE_MAP_PATH`: 서비스 메타 CSV 경로(선택)
  - 헤더: `service,service_group,is_active,description`

**크론 예시**
```bash
crontab -e
0 2 * * * /home/kang/log-etlm/scripts/dim_spark_batch.sh >> /home/kang/log-etlm/logs/dim_batch.log 2>&1
```

#### 2. ClickHouse SQL 배치 (Spark 없이 간단 실행)

```bash
# DIM_RESET=1이면 TRUNCATE 후 재적재(기본 1)
DIM_LOOKBACK_DAYS=7 DIM_RESET=1 bash scripts/dim_clickhouse_batch.sh
```


### Kafka 토픽 파티션 분배(도메인 기준)

`KAFKA_AUTO_CREATE_TOPICS_ENABLE=true`여도 기본 파티션은 1개이므로,
초기 한 번은 명시적으로 생성하고 이후 `--alter --partitions N`으로 증설한다.

```bash
# 생성 예시(자동 생성이 켜져 있어도 명시적으로 1회)
docker exec -it kafka kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic logs.auth --partitions 5 --replication-factor 1

# 증설 예시(줄이기 불가)
docker exec -it kafka kafka-topics --bootstrap-server kafka:9092 --alter --topic logs.auth --partitions 5
docker exec -it kafka kafka-topics --bootstrap-server kafka:9092 --alter --topic logs.order --partitions 3
docker exec -it kafka kafka-topics --bootstrap-server kafka:9092 --alter --topic logs.payment --partitions 2

# 확인
docker exec -it kafka kafka-topics --bootstrap-server kafka:9092 --describe --topic logs.auth
```

> 파티션 감소는 불가하므로 필요 시 토픽 삭제 후 재생성한다.


## 프로파일 & 튜닝 포인트

### 시뮬레이터

- 부하 프로파일/라우트 설정은 `log_simulator/config/{profiles.yml,routes.yml}` 참고
- 런타임 설정: `docker-compose.yml`의 simulator environment
  - `TARGET_INTERVAL_SEC`, `LOG_BATCH_SIZE`, `LOOPS_PER_SERVICE`, `PUBLISHER_WORKERS`
  - `SIMULATOR_SHARE`, `SIM_BEHIND_LOG_EVERY_SEC`

### Spark 스트리밍

- 환경 프로파일 값은 `config/env/{low,mid,high}.env.example` 참고(적용은 `docker-compose.yml`)
- 스트림 분리/동작: `docker-compose.yml`
  - `SPARK_FACT_TOPICS`, `SPARK_DLQ_TOPIC`, `SPARK_ENABLE_DLQ_STREAM`, `SPARK_STARTING_OFFSETS`, `SPARK_STORE_RAW_JSON`
  - `SPARK_FACT_TRIGGER_INTERVAL`, `SPARK_RESET_CHECKPOINT_ON_START`
  - `SPARK_DLQ_TRIGGER_INTERVAL`, `SPARK_DLQ_KAFKA_TRIGGER_INTERVAL`, `SPARK_DLQ_KAFKA_LOG_EMPTY`
  - `SPARK_BATCH_TIMING_LOG_PATH`, `SPARK_PROGRESS_LOG_PATH`

### Spark 배치(DIM)

- spark-batch 환경: `docker-compose.yml`
  - `SPARK_BATCH_MASTER`, `SPARK_BATCH_DRIVER_MEMORY`, `SPARK_BATCH_EXECUTOR_MEMORY`, `SPARK_BATCH_SHUFFLE_PARTITIONS`
  - `DIM_BATCH_LOOKBACK_DAYS`, `DIM_SERVICE_MAP_PATH` (상세는 “DIM 배치” 섹션 참고)

### ClickHouse

- 적재 튜닝: `SPARK_CLICKHOUSE_WRITE_PARTITIONS`, `SPARK_CLICKHOUSE_JDBC_BATCHSIZE`
- 읽기/리파티션: `SPARK_CLICKHOUSE_JDBC_FETCHSIZE`, `SPARK_CLICKHOUSE_ALLOW_REPARTITION`
- 서버/사용자 설정: `infra/clickhouse/`
  - `config.d/` (예: listen_host, async insert 로그)
  - `users.d/` (예: log_user async_insert)

### Kafka

- 보관/메모리: `KAFKA_HEAP_OPTS`, `KAFKA_LOG_RETENTION_HOURS`, `KAFKA_LOG_RETENTION_BYTES`, `KAFKA_LOG_SEGMENT_BYTES`, `KAFKA_NUM_PARTITIONS`

### near-real-time 운용 팁(단일 VM 기준)

- CPU 포화 시 `SPARK_FACT_TRIGGER_INTERVAL`과 `SPARK_MAX_OFFSETS_PER_TRIGGER`를 조정해 지연(SLA)을 안정적으로 맞추는 것을 우선한다.
- 실시간(10s) 집계가 과부하를 유발하면 10s MV를 DETACH해 운영(1m) 지표 중심으로 관찰한다.

### 유틸 스크립트 목록

- Spark 프로파일: `scripts/apply_spark_env.sh`, `scripts/current_spark_env.sh`, `scripts/autoswitch_spark_env.sh`
- DIM 배치: `scripts/dim_spark_batch.sh`, `scripts/dim_clickhouse_batch.sh`
- Kafka/Spark 지연: `scripts/check_backlog.sh`, `scripts/kafka_spark_lag.py`
- ClickHouse 진단: `scripts/clickhouse_diagnostics.sh`, `scripts/diag_partition_and_partlog.sh`
- 스파이크 분석: `scripts/publish_spike_diag.sh`


## 지표 해석(요약)

- ingest: `event_ts → ingest_ts` (수집 지연)
- process: `ingest_ts → processed_ts` (Spark 처리 지연)
- sink: `processed_ts → stored_ts` (Spark → ClickHouse 적재 지연)
- end-to-end(ops): `ingest_ts → stored_ts` (전체 지연)
- 생성 대비 적재 비율: `created_ts` 대비 `stored_ts` 비율 (1분 버킷 기준, 지연이 크면 0%로 보일 수 있음)
- DLQ: `analytics.fact_event_dlq_agg_1m` (service, error_type, total)


## ClickHouse 집계/권한/백필 가이드

[ClickHouse 집계/권한/백필 가이드](docs/clickhouse_aggregate_guide.md)
