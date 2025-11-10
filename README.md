# Log-Forge: stdout → Fluent Bit → Kafka 파이프라인 전환 가이드

애플리케이션 **stdout(JSON 1-line)** 로그를 **Fluent Bit**가 수집하여 **Kafka**로 전달하는 구조입니다.  
목표는 **SDK 의존 최소화(디커플링)**, **운영 안정성(오프셋/백로그)**, **멀티 싱크 확장성**입니다.

---

## ✅ 최종 아키텍처


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
mkdir -p fluent-bit/storage fluent-bit/state

# 1) 서비스 기동
docker compose up -d kafka
docker compose up -d kafka-ui fluent-bit
docker compose up -d --build simulator

# 2) 상태 확인
docker logs -f fluent-bit
docker logs -f simulator | head -n 5
```

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
- `http://localhost:8080` → Topics

