# Log-Forge: stdout → Fluent Bit → Kafka 파이프라인 전환 가이드

애플리케이션 **stdout(JSON 1-line)** 로그를 **Fluent Bit**가 수집하여 **Kafka**로 전달하는 구조입니다.  
목표는 **SDK 의존 최소화(디커플링)**, **운영 안정성(오프셋/백로그)**, **멀티 싱크 확장성**입니다.

---

## ✅ 최종 아키텍처

```mermaid
flowchart LR
  A[Simulator (stdout, JSON 1-line)] -->|tail /var/lib/docker/containers/*/*.log| B[Fluent Bit v4]
  B -->|parser: docker → parser: json(log) → modify(remove log)| C[정규화된 레코드]
  C -->|topic_key=topic, message_key_field=key, timestamp_key=ts| D[Kafka (logs.*)]
  D --> E[Kafka UI / Consumers / OLAP]
```

---

## 📂 변경 사항(요약)

```perl
.
├── docker-compose.yml                 # Fluent Bit 추가, 로깅/네트워크 통일
├── dev/
│   └── fluent-bit/
│       ├── fluent-bit.conf            # v4 호환(parser/modify → kafka)
│       ├── storage/                   # filesystem backlog
│       └── state/                     # tail 오프셋 DB
└── simulator/
    ├── core/
    │   └── kafka.py                   # SSOT: SINK(stdout|kafka), StdoutProducer(envelope), gzip 설정
    └── producer.py                    # SSOT Producer만 사용하도록 슬림화
```

---

## 🧩 핵심 포인트

- Simulator → stdout(JSON 1-line)
    - 기본값 SIM_SINK=stdout
    - _StdoutProducer가 {"ts","topic","key","value"} 형태로 출력
    - 내부 json.loads로 value를 객체화해 배출

- Fluent Bit v4 파이프라인
    - 입력: Tail /var/lib/docker/containers/*/*.log + filesystem backlog/offset DB
    - 필터:
        - parser(Key_Name=log, Parser=json) → log 필드 승격
        - modify(Remove log) → 원본 log 제거
    - 출력: Kafka
        - topic_key topic, message_key_field key, timestamp_key ts, timestamp_format iso8601
        - 이미지 zstd 미포함 → compression.type zstd 미사용(필요 시 gzip|lz4|none)
- Kafka 직접 전송은 옵션
    - 필요 시 SIM_SINK=kafka + confluent-kafka 설치

---

## ⚙️ 사용 방법

```bash
# 0) 디렉터리 준비(오프셋/백로그)
mkdir -p dev/fluent-bit/storage dev/fluent-bit/state

# 1) 서비스 기동
docker compose up -d zookeeper kafka
docker compose up -d kafka-ui fluent-bit
docker compose up -d --build simulator

# 2) 상태 확인
docker logs -f fluent-bit
docker logs -f simulator | head -n 5
```

> Windows/Mac: /var/lib/docker/containers 경로 tail이 어려우면 Docker Engine API 입력으로 전환 필요(본 구성은 Linux 표준 경로 기준).

---

## 🧾 Fluent Bit 설정

`dev/fluent-bit/fluent-bit.conf`:
```ini
[SERVICE]
    flush                     1
    log_level                 info
    parsers_file              /fluent-bit/etc/parsers.conf
    storage.path              /fluent-bit/storage
    storage.sync              normal
    storage.backlog.mem_limit 256MB

[INPUT]
    Name                tail
    Path                /var/lib/docker/containers/*/*.log
    Tag                 docker.*
    Parser              docker
    DB                  /fluent-bit/state/tail.db
    Mem_Buf_Limit       256MB
    Skip_Long_Lines     On
    Refresh_Interval    5
    Rotate_Wait         10
    storage.type        filesystem

# 컨테이너 로그의 "log" JSON을 top-level로 승격
[FILTER]
    Name          parser
    Match         docker.*
    Key_Name      log
    Parser        json
    Reserve_Data  On

# 승격 후 원본 log 키 제거
[FILTER]
    Name      modify
    Match     docker.*
    Remove    log

# 공통 태그(선택)
[FILTER]
    Name    record_modifier
    Match   docker.*
    Record  cluster local
    Record  pipeline app-stdout

[OUTPUT]
    Name                 kafka
    Match                docker.*
    Brokers              kafka:9092
    topics               logs.app
    topic_key            topic
    message_key_field    key
    timestamp_key        ts
    timestamp_format     iso8601
    # rdkafka.compression.type  gzip   # 필요 시 사용(zstd는 미포함 이미지)
    rdkafka.request.required.acks  1
    rdkafka.log.connection.close   false
```

> 전체 envelope까지 보내고 싶다면: format json을 추가하면 됩니다(현재는 value만 payload로 전달되는 형태를 권장).

---

## 🔧 Simulator 코드(핵심)

`simulator/core/kafka.py` (발췌):
- SINK = `os.environ.get("SIM_SINK", "stdout").lower()`
- `_StdoutProducer.produce()` → envelope 1-line JSON 출력
- `get_producer_config()` → `compression.type="gzip"`(호환성)
- `Producer` 심볼을 SSOT에서 직접 제공(stdout 모드 시 `_StdoutProducer` 폴백)

`simulator/producer.py`:
- `from simulator.core.kafka import Producer` 만 사용(SSOT 일원화)
- `build_producer(config) → Producer(config)`
- `produce(...) → prod.produce(...)` 위임
- `flush_safely(...)`는 stdout 모드에서 사실상 no-op

---

## 🧪 검증

### 1) 시뮬레이터 stdout
```bash
docker logs -f simulator | head -n 3
# {"ts":"...","topic":"logs.notify","key":"req_xxx","value":{"ts":"...","svc":"notify","evt":"..."...}}
```

### 2) Kafka 컨슈머
```bash
docker compose exec kafka \
  kafka-console-consumer --bootstrap-server kafka:9092 \
  --topic logs.notify --from-beginning --max-messages 3
```

### 3) Kafka UI
- `http://localhost:8080` → Topics → `logs.notify`(동적) 또는 `logs.app`(기본)

---

## 🛠️ 트러블슈팅

- docker 필터 에러
    - 본 구성은 v4에서 docker 필터 미사용. parser/modify 조합 사용.
    - [SERVICE] parsers_file 누락 시 Parser docker 에러 발생 → 경로 확인.
- filesystem storage 에러
    - [SERVICE] storage.path 누락/권한 문제.
    - dev/fluent-bit/storage, dev/fluent-bit/state 생성/권한 확인.
- Topics_Key 인식 실패
    - v4는 Topics_Key가 아니라 topic_key 사용.
- zstd not available
    - 사용 이미지에 zstd 미포함 → rdkafka.compression.type zstd 제거 또는 **gzip**으로 변경.
- Local: Queue full
    - 이는 Kafka 직접 전송 모드에서만 발생.
    - 본 구조는 SIM_SINK=stdout으로 동작 → 무관.
    - (직접 전송이 필요하면 poll(0)/버퍼 튜닝 필수)
- Windows/Mac에서 tail 실패
    - /var/lib/docker/containers 마운트 구조 상이 → Docker API 입력으로 전환 필요.

---

## 🧱 운영 팁(선택)

- 토픽/보존/파티션 기본값
```bash
docker compose exec kafka kafka-topics \
  --create --if-not-exists --bootstrap-server kafka:9092 \
  --topic logs.notify --partitions 3 --replication-factor 1 \
  --config retention.ms=604800000
```

- value.*를 최상위로 승격(OLAP 쿼리 편의)
```bash
[FILTER]
    Name    nest
    Match   docker.*
    Operation lift
    Nested_under value
```

- 멀티 싱크 확장(ES/ClickHouse/S3 등)
    - Fluent Bit에 [OUTPUT]를 추가해 앱 변경 없이 다중 전송 가능.

---
