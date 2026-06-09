# 기술 스택 선택 근거

이 문서는 대용량 로그 처리 파이프라인 PoC에서 각 기술을 선택한 이유, 장단점, 그리고 대안 스택과의 비교를 정리합니다.

---

## 프로젝트 맥락

**목표**: 대용량 이벤트 로그(~5,000 EPS)를 수집·처리·시각화하는 near-real-time 파이프라인 검증  
**제약**: 단일 VM(vCPU 7, Docker Compose) 환경. 컴포넌트 간 자원 경합으로 초저지연 실시간은 비현실적이며, **E2E 지연 p95 ~10초를 목표**로 설계됨.

---

## 1. FastAPI

### 선택 이유

시뮬레이터를 단순 이벤트 생성 스크립트가 아닌 **HTTP로 제어 가능한 서비스**로 구축하는 것이 목표였다. EPS 조정, 부하 프로파일 변경, 상태 조회 등의 제어 인터페이스를 런타임에 노출하려면 HTTP 서버 프레임워크가 필요했고, 동시에 Kafka 퍼블리셔(네트워크 I/O 바운드)를 효율적으로 구동하려면 Python 비동기 런타임이 필요했다.

- **HTTP 제어 인터페이스**: EPS 조정·상태 조회·부하 프로파일 변경 등을 런타임에 API로 노출. 라우터 추가만으로 제어 범위 확장 가능
- **`asyncio` lifespan**: 앱 기동/종료 훅(`@asynccontextmanager lifespan`)으로 토큰 버킷 루프와 퍼블리셔 워커를 HTTP 서버 생명주기에 바인딩. 별도 프로세스 관리 불필요
- **`asyncio.Queue` 기반 backpressure**: 큐 점유율에 따른 2단계 backpressure(soft 85% scale down / hard 90% drain 대기)를 락 없이 단일 스레드에서 구현
- **비동기 I/O 효율**: Kafka 퍼블리셔는 네트워크 I/O 바운드 작업. 스레드 기반 대비 컨텍스트 스위치 비용이 낮은 `asyncio` 코루틴이 적합

### 장점

- lifespan 이벤트로 엔진 생명주기를 서버와 일치시켜 기동·종료 관리 단순화
- `loops_per_service`로 서비스당 N개 루프를 `asyncio.gather`로 병렬 실행해 단일 스레드 한계를 우회

### 단점

- **프레임워크 과잉**: 현재 구조에서 HTTP 제어 API가 실제로 활용되지 않고 있어, 순수 asyncio 스크립트로도 동일한 기능 구현 가능
- **GIL 제약**: 단일 uvicorn 워커(`--workers 1`) 환경에서 CPU 집약 작업 발생 시 병목 가능
- **단일 워커 한계**: 시뮬레이터 컨테이너 자체가 cpus 1.0으로 제한되어 있어 멀티 워커 이점 없음

### 대안 비교

| 대안                      | 비교                                                                                                                            |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| **순수 asyncio 스크립트** | 가장 경량. 그러나 HTTP 기반 상태 조회·제어 엔드포인트가 필요한 시점에 재구조화 필요                                             |
| **Locust**                | 부하 생성 도구로 EPS 제어 기능 내장. 그러나 Kafka 직접 발행, 커스텀 이벤트 스키마, 토큰 버킷 로직을 구현하기 위한 유연성이 낮음 |

---

## 2. Apache Kafka

### 선택 이유

Kafka는 **분산 로그 스트리밍 플랫폼**이다. 메시지를 디스크에 순차 기록하고, 소비 후에도 보존 기간 동안 유지한다. 이 특성 덕분에 소비자가 과거 오프셋으로 되돌아가 재처리할 수 있으며, 여러 소비자가 같은 토픽을 독립적으로 읽을 수 있다. 주로 고처리량 이벤트 스트리밍, 로그 수집, 마이크로서비스 간 비동기 통신에 사용된다.

이 프로젝트에서 Kafka가 필요한 이유는 두 가지다. 첫째, 시뮬레이터(생산자)와 Spark(소비자) 사이의 처리 속도 차이를 흡수할 버퍼가 필요하다. EPS 스파이크 시에도 Spark는 자기 페이스로 소비할 수 있어야 한다. 둘째, RabbitMQ 같은 단순 메시지 큐는 소비 즉시 메시지를 삭제하므로, Spark 장애 후 재처리가 불가능하다. Kafka는 메시지를 보존하므로 **장애 복구 시 오프셋부터 재개해 데이터 유실 없이 파이프라인을 재건**할 수 있다.

- **오프셋 기반 재처리**: Spark 체크포인트(`checkpointLocation`)와 결합해 장애 복구 시 마지막 오프셋부터 재개 가능. at-least-once 보장. 단, 현재 PoC 설정(`SPARK_RESET_CHECKPOINT_ON_START=true`)에서는 재시작 시 체크포인트를 초기화하므로 항상 최신 오프셋부터 시작하는 모드로 운영 중 — 재처리 기능 자체는 구현되어 있으나 PoC에서는 비활성 상태
- **토픽 분리**: 서비스별 토픽(`logs.auth`, `logs.order`, `logs.payment`)으로 독립 소비, `logs.dlq`로 파싱 실패 이벤트 우회 경로 자연스럽게 구성
- **KRaft 모드**: ZooKeeper 의존성 제거. 단일 VM에서 컴포넌트 하나를 줄여 자원 절감
- **`maxOffsetsPerTrigger` 연동**: Spark 소비 속도를 직접 제어해 다운스트림(ClickHouse) 과부하 방지

### 장점

- 높은 처리량과 낮은 지연. 파티션 수 조정만으로 병렬 처리 규모 확장 가능
- 생산자-소비자 결합도를 낮춰 각 컴포넌트를 독립적으로 스케일링·장애격리 가능
- 로그 보존 기간(`KAFKA_LOG_RETENTION_HOURS=2`) 설정으로 PoC 환경의 디스크 사용량을 제어하면서도 단기 장애 복구 구간은 확보

### 단점

- **단일 브로커 한계**: 브로커 장애 시 전체 파이프라인 중단. 실 운영이라면 최소 3개 브로커 + 복제 구성 필요
- **메모리 소비 큼**: `-Xms1g -Xmx1536m` 설정으로 단일 VM에서 다른 컴포넌트와 메모리 경합 발생
- **보존 기간 초과 장애는 재처리 불가**: 보존 기간 2시간은 디스크 절감을 위한 PoC 설정값으로, 이를 초과한 장애 구간의 데이터는 복구 불가능

### 대안 비교

| 대안            | 비교                                                                                                                  |
| --------------- | --------------------------------------------------------------------------------------------------------------------- |
| **RabbitMQ**    | 큐 기반으로 메시지 소비 후 삭제됨. 오프셋 재처리 불가. 로그 스트리밍 용도로 부적합                                    |
| **AWS Kinesis** | 관리형 서비스로 운영 부담 낮음. 그러나 단일 VM 로컬 환경 구성 불가. PoC 재현성 낮아짐                                 |
| **Pulsar**      | Kafka와 유사한 스트리밍 기능 + 기본 멀티 테넌시 지원. 그러나 생태계·레퍼런스가 Kafka 대비 부족하고 설정 복잡도가 높음 |

---

## 3. Apache Spark Structured Streaming

### 선택 이유

Spark는 **분산 데이터 처리 엔진**이다. Structured Streaming은 Kafka 같은 스트림 소스를 마이크로배치로 소비하면서, 파싱·변환·집계·싱크 적재를 선언적 DataFrame API로 표현한다. 체크포인트를 통한 장애 복구, 소비 속도 제어(배압), 다양한 싱크 연동을 프레임워크 수준에서 내장 지원한다. 주로 대규모 배치 처리, ETL 파이프라인, 스트림 집계에 사용된다.

이 프로젝트에서 Kafka와 ClickHouse 사이에는 단순 소비자가 아닌 **ETL 레이어**가 필요했다. 원본 이벤트를 파싱·검증·정규화한 뒤 ClickHouse에 안정적으로 적재해야 하고, ClickHouse는 트랜잭션을 지원하지 않아 멱등성을 애플리케이션 레벨에서 직접 구현해야 한다. Spark의 `foreachBatch`는 마이크로배치 단위로 임의 Python 코드를 실행할 수 있어, JDBC 적재와 `stream_batch_guard` 중복 방지 체크를 같은 흐름 안에 자연스럽게 결합할 수 있었다. Flink의 커스텀 싱크나 Kafka Streams의 Processor API로도 유사하게 구현할 수 있지만, `foreachBatch` 패턴이 Python 코드로 가장 직관적으로 표현된다.

- **체크포인트 기반 장애 복구**: 재시작 시 마지막 Kafka 오프셋과 처리 상태를 자동 복원. 별도 복구 로직 구현 불필요
- **`foreachBatch` 커스텀 싱크**: 마이크로배치 단위로 임의 Python 코드를 실행할 수 있어, JDBC 적재·dedup·타이밍 로그·빈 배치 스킵 등을 같은 흐름 안에 결합 가능. 표준 JDBC 싱크로는 이 커스텀 로직 삽입이 불가능
- **`dropDuplicates` dedup**: 배치 단위 중복 제거를 상태 저장소 없이 처리해 state I/O 오버헤드 제거. 워터마크 기반 dedup은 `SPARK_FACT_DEDUP_WATERMARK` 설정으로 선택 가능
- **`maxOffsetsPerTrigger` 배압**: 트리거당 소비할 Kafka 오프셋 수를 제한해 ClickHouse 과부하를 Spark 레벨에서 선제 방어

### 장점

- 선언적 스트리밍 API로 파싱 → 검증 → 정규화 → 적재 파이프라인을 단계별로 명확히 분리
- `SPARK_FACT_TRIGGER_INTERVAL`, `SPARK_MAX_OFFSETS_PER_TRIGGER` 등 환경변수로 부하에 맞게 동적 튜닝 가능
- Python 생태계(PySpark)를 그대로 활용 가능해 시뮬레이터·공통 모듈과 언어 통일

### 단점

- **JVM 메모리 압박**: worker 2개(각 3g 제공, executor heap 2g), driver 1GB 등 단일 VM에서 최대 메모리 소비 컴포넌트. CPU 포화의 주요 원인
- **PySpark 직렬화 오버헤드**: Python ↔ JVM 간 데이터 직렬화 비용 존재. Scala/Java 대비 처리량 불리
- **마이크로배치 한계**: 4초 트리거 + 단일 VM 자원 포화로 E2E 지연이 ~10초 수준. 레코드 단위 처리가 필요한 진정한 실시간에는 부적합
- **Shuffle 수동 튜닝 필요**: `SPARK_STREAM_SHUFFLE_PARTITIONS`(기본값 3) 등 환경에 따라 수동 조정 필요

### 대안 비교

| 대안                                   | 비교                                                                                                                                                                  |
| -------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Apache Flink**                       | 진정한 이벤트 단위 스트리밍(레코드 단위 처리)으로 지연이 Spark 대비 낮음. 그러나 Python API 성숙도가 낮고, 단일 VM에서 JobManager + TaskManager 구성이 Spark보다 복잡 |
| **Kafka Streams**                      | Kafka와 네이티브 통합으로 별도 클러스터 불필요. 그러나 Java 전용이고, ClickHouse JDBC 싱크 구성이 Spark 대비 번거로움                                                 |
| **직접 구현 (confluent-kafka-python)** | 경량. 그러나 워터마크 dedup, 체크포인트, 분산 처리를 직접 구현해야 해 PoC 범위를 크게 벗어남                                                                          |

---

## 4. ClickHouse

### 선택 이유

ClickHouse는 **컬럼 지향 OLAP 데이터베이스**다. 데이터를 행이 아닌 컬럼 단위로 저장하므로, 특정 컬럼만 집계하는 분석 쿼리에서 디스크 I/O를 최소화한다. Materialized View와 AggregatingMergeTree 엔진을 결합하면 INSERT 시점에 집계를 자동 갱신할 수 있어 별도 집계 잡 없이 실시간 집계 테이블을 유지할 수 있다. 주로 로그 분석, 실시간 대시보드, 시계열 집계 등 대용량 읽기·분석 워크로드에 사용된다.

이 프로젝트에서는 **Spark의 지속적인 INSERT와 Grafana의 집계 쿼리가 동시에 발생**한다. 5,000 EPS 수준의 쓰기 부하 아래에서도 Grafana가 EPS·지연·에러율을 낮은 응답 지연으로 읽어야 한다. PostgreSQL 같은 행 스토어는 이 조건에서 집계 쿼리 성능이 급격히 저하되고, Elasticsearch는 집계 쿼리보다 텍스트 검색에 최적화되어 있다. ClickHouse는 컬럼 스토리지로 INSERT와 집계 쿼리 경합을 최소화하면서 Materialized View로 별도 집계 잡 없이 실시간 집계를 유지할 수 있어 이 요구사항에 가장 적합했다.

- **Wide Table + MV 서빙 모델**: 킴벌 Star Schema(Fact + Dimension JOIN)는 RDBMS 행 스토어의 한계를 극복하기 위한 패턴이다. ClickHouse는 컬럼 스토어 기반으로 필요한 컬럼만 읽으므로 Wide Table로 비정규화해도 I/O 비용이 없다. `event_log`에 모든 속성을 저장하고, Dimension 테이블 없이 MV만으로 집계를 처리한다
- **Materialized View 자동 집계**: `event_log` INSERT 시 MV가 자동으로 1분/10초 집계 테이블을 갱신. Spark는 원본 데이터만 쓰면 되고, 별도 집계 잡 스케줄링이 불필요
- **`AggregatingMergeTree` + `quantileTDigest`**: p95 지연을 컬럼 스토리지 레벨에서 점진적으로 집계. Grafana 쿼리 시 집계 테이블만 SELECT하면 되어 응답 속도 빠름
- **`async_insert` 프로파일**: Spark JDBC 소량 배치를 비동기 버퍼링해 INSERT 피크를 흡수. `log_user`에만 적용해 ingest 경로를 읽기 경로(`grafana_user`)와 자원 격리. `wait_for_async_insert=0`으로 설정해 JDBC `.save()` 반환을 flush 완료가 아닌 버퍼 수신 완료 시점으로 둠 — **throughput 우선 설계 선택**. flush 직전 재시작 시 데이터 유실 위험이 있으나 MergeTree 버퍼 손실은 실운영 기준으로도 드문 케이스여서 수용한다
- **`stream_batch_guard` 테이블**: `foreachBatch` 재실행 시 중복 배치를 DB 레벨에서 skip해 Spark의 at-least-once를 effectively-once로 보완. 단, 현재 PoC 설정(`SPARK_RESET_CHECKPOINT_ON_START=true`, `BATCH_GUARD_ENABLED=false`)에서는 비활성 상태 — 체크포인트를 보존하는 운영 환경 전환 시 활성화하면 크래시 복구 시 중복 적재를 방어할 수 있음

### 장점

- 컬럼 스토리지 + `LowCardinality(String)` 타입으로 반복값 컬럼(service, event_name 등) 압축률·조회 성능 향상
- `Delta + ZSTD` 코덱으로 타임스탬프 컬럼 압축 최대화
- TTL 정책(`event_log` 1일, `event_log_dlq` 7일 등)으로 디스크 사용량을 자동 관리

### 단점

- **MV INSERT 경합**: INSERT마다 7개 MV가 동기 실행되어 Grafana 읽기 쿼리와 CPU 경합 발생. 중복 MV 제거(9개→7개) 및 async_insert flush 간격 확대로 부분 완화. 10초 MV는 부하 시 DETACH 가능
- **JDBC 기반 연동 한계**: Spark-ClickHouse 간 네이티브 커넥터 대비 JDBC 처리량이 제한적
- **비동기 dedup**: MergeTree 파트 병합이 비동기로 처리되어 즉각적인 중복 제거가 보장되지 않음. Spark 레벨 dedup을 반드시 병행해야 함
- **단일 노드 한계**: 장애 시 읽기·쓰기 모두 중단. 실 운영이라면 Replica 구성 필요

### 대안 비교

| 대안                         | 비교                                                                                                       |
| ---------------------------- | ---------------------------------------------------------------------------------------------------------- |
| **Elasticsearch**            | 로그 검색에 강점. 그러나 집계 쿼리 성능이 ClickHouse 대비 낮고, 메모리 소비가 커서 단일 VM에서 공존이 부담 |
| **Apache Druid**             | 실시간 OLAP에 특화. 그러나 컴포넌트가 많아(Broker/Historical/Coordinator 등) 단일 VM PoC에 과도하게 복잡   |
| **PostgreSQL + TimescaleDB** | 친숙한 SQL 환경. 그러나 컬럼 스토리지 기반이 아니어서 대용량 집계 쿼리 성능이 ClickHouse 대비 불리         |

---

## 5. Grafana

### 선택 이유

ClickHouse 집계 테이블을 실시간으로 시각화하는 대시보드가 필요했다.

- **`grafana-clickhouse-datasource`**: ClickHouse 전용 공식 플러그인으로 집계 테이블에 SQL을 직접 쿼리. 별도 API 레이어나 데이터 변환 없이 바로 패널 구성 가능
- **프로비저닝 코드화**: datasource(`provisioning/datasources/`), dashboard JSON(`provisioning/dashboards/`)을 파일로 관리해 컨테이너 재기동 시 자동 복원

### 장점

- 대시보드별 새로고침 주기 분리(ops 1분 / realtime 30초)로 ClickHouse 읽기 부하를 의도적으로 제한. INSERT 지연 스파이크 사고 이후 주기 확대(ops 30초→1분, realtime 10초→30초)
- `ops_monitoring.json`(1분 집계), `realtime.json`(10초 집계) 2개 대시보드로 운영·실시간 가시성 분리

### 단점

- **단일 인스턴스 SPOF**: Grafana 장애 시 모니터링 화면 전체 불가. 복구 전까지 맹점 구간 발생
- **플러그인 버전 의존성**: `grafana-clickhouse-datasource` 플러그인이 Grafana 버전 업그레이드 시 호환성 문제가 생길 수 있어 버전을 고정(`grafana:10.4.2`)해 운영

### 대안 비교

| 대안                         | 비교                                                                                                                                    |
| ---------------------------- | --------------------------------------------------------------------------------------------------------------------------------------- |
| **Apache Superset**          | ClickHouse 연동 가능하고 SQL 기반 대시보드 구성 가능. 그러나 설치·관리 복잡도가 Grafana 대비 높고, 스트리밍 실시간 새로고침 지원이 약함 |
| **Metabase**                 | 비기술자 친화적 UI. 그러나 ClickHouse 공식 지원이 미흡하고 커스텀 쿼리 유연성 부족                                                      |
| **직접 구현 (Streamlit 등)** | 완전한 커스터마이징 가능. 그러나 대시보드·알림·프로비저닝을 직접 구현해야 해 PoC 범위를 벗어남                                          |
