# OOM 방지 체크리스트

이 문서는 단일 VM 환경에서 OOM 위험을 빠르게 판단하고 즉시 완화하는 운영 체크리스트입니다.

## 공통 원칙
- 입력(ingest)을 줄이는 게 가장 빠른 완화 방법이다.
- 배치 크기를 줄이면 대부분의 OOM 위험이 즉시 감소한다.
- 쿼리 폭주 방지(ClickHouse/Grafana)는 비용 대비 효과가 크다.
- 스왑이 없으면 순간 스파이크에 특히 민감하다.
- 컨테이너별 `mem_limit`/`mem_reservation`과 `oom_score_adj`로 “살릴 서비스 우선순위”를 정책화한다.

## 컴포넌트별 체크리스트

### 1) Simulator
**위험 신호**
- publish 큐 사용률이 80% 이상으로 지속
- send latency 증가 또는 queue full 로그 발생
- EPS가 급격히 상승

**즉시 조치**
- `TARGET_INTERVAL_SEC` 증가(발행 속도 감소)
- `SIMULATOR_SHARE` 감소
- `QUEUE_THROTTLE_RATIO`, `QUEUE_THROTTLE_SLEEP` 강화
- `PRODUCER_QUEUE_MAX_MESSAGES`, `PRODUCER_QUEUE_MAX_KBYTES` 축소
- simulator 컨테이너 `mem_limit`/`oom_score_adj` 정책 점검(과부하 시 우선 종료 대상)

### 2) Kafka
**위험 신호**
- GC 빈도 증가, latency 급증
- 디스크 I/O 대기 증가
- 브로커 로그에 OOM/GC 메시지

**즉시 조치**
- 시뮬레이터 발행 속도 감소(위 Simulator 조치)
- 파티션 수 확대(스큐 완화)
- `KAFKA_HEAP_OPTS`와 컨테이너 메모리 상한 불일치 점검

### 3) Spark Driver (Streaming)
**위험 신호**
- 마이크로 배치 처리 시간 증가
- `inputRate > processedRate` 지속
- 드라이버 로그에 OOM/GC 메시지

**즉시 조치**
- `SPARK_MAX_OFFSETS_PER_TRIGGER` 낮추기
- `SPARK_MAX_OFFSETS_CAP` 보수적으로 설정(기본 30000)
- `SPARK_FACT_TRIGGER_INTERVAL` 늘리기
- 필요 시 `SPARK_STREAM_DRIVER_MEMORY`, `SPARK_STREAM_EXECUTOR_MEMORY` 상향(여유 있을 때만)
- `SPARK_MAX_OFFSETS_SAFETY`를 낮춰 자동 산정치를 보수화(기본 1.1)

### 4) Spark Executors / Workers
**위험 신호**
- shuffle spill 증가
- 특정 파티션 처리 시간이 비정상적으로 길어짐(스큐)
- 워커 로그에 OOM/GC 메시지

**즉시 조치**
- `SPARK_CLICKHOUSE_WRITE_PARTITIONS` 증가
- Kafka 파티션 수 증가(스큐 완화)
- 워커 메모리 상향(가능 시)
- `SPARK_MAX_OFFSETS_PER_TRIGGER` 낮춰 배치 크기 축소
- ClickHouse insert 부담이 클 때 `SPARK_CLICKHOUSE_JDBC_BATCHSIZE` 축소

### 5) Spark Batch (DIM)
**위험 신호**
- 배치 실행 시간이 급격히 증가
- executor/driver OOM 로그
- shuffle spill 폭증

**즉시 조치**
- `DIM_BATCH_LOOKBACK_DAYS` 축소
- `SPARK_BATCH_SHUFFLE_PARTITIONS` 증가
- `SPARK_BATCH_DRIVER_MEMORY`, `SPARK_BATCH_EXECUTOR_MEMORY` 상향

### 6) ClickHouse
**위험 신호**
- 쿼리 타임아웃, `Code: 241` 발생
- insert latency 급증, MV 처리 지연
- 메모리 사용 급증

**즉시 조치**
- Grafana 조회 범위 축소, refresh 주기 증가
- `max_memory_usage`, `max_memory_usage_for_user` 상향 또는 조정(현재 4GiB)
- `max_bytes_before_external_group_by/sort`로 spill 유도(현재 256MB)
- 10s MV 비활성화(부하 감소)
- ClickHouse 컨테이너 `mem_limit` 상향(현재 4g) 및 여유 확인

### 7) Grafana / UI
**위험 신호**
- 대시보드 로딩 지연
- 쿼리 실패/timeout 증가

**즉시 조치**
- 대시보드 time range 축소
- panel refresh 주기 증가
- 고해상도(10s) 패널 비활성화

## 운영 팁
- OOM은 “발생 후 복구”보다 “발생 전 제한/캡”이 훨씬 저렴하다.
- 처리량 우선 환경일수록 `SPARK_MAX_OFFSETS_CAP`은 안전장치로만 두고, 실제 조정은 `TARGET_INTERVAL_SEC`/`SPARK_MAX_OFFSETS_PER_TRIGGER`로 한다.
