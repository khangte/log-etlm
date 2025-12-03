# Log Monitoring PoC Project

## 개요
마이크로서비스에서 발생하는 HTTP 액세스 로그를 실시간으로 수집‧가공‧시각화하는 파이프라인 PoC입니다. FastAPI 기반 시뮬레이터가 Kafka 로그 토픽에 다양한 서비스 패턴을 발행하면, Spark Structured Streaming 잡이 이를 ClickHouse 분석 테이블로 적재하고 Grafana 대시보드로 노출합니다. 각 컴포넌트는 Docker Compose로 손쉽게 기동할 수 있으며, ClickHouse 초기 스키마와 Grafana 프로비저닝도 자동화되어 있어 부팅 직후부터 엔드투엔드 흐름을 검증할 수 있습니다.

## 기술 스택
- <img src="https://img.shields.io/badge/FastAPI-009688?style=for-the-badge&logo=fastapi&logoColor=white">  
    FastAPI: `log_gateway` 시뮬레이터 및 API 엔드포인트
- <img src="https://img.shields.io/badge/ApacheKafka-231F20?style=for-the-badge&logo=apachekafka&logoColor=white"> 
    Apache Kafka + Kafka UI: 로그 수집 버퍼와 모니터링 UI
- <img src="https://img.shields.io/badge/ApacheSpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white"> 
    Apache Spark 4.0 Structured Streaming: Kafka → ClickHouse 실시간 적재
- <img src="https://img.shields.io/badge/ClickHouse-FFCC01?style=for-the-badge&logo=clickhouse&logoColor=white"> 
    ClickHouse: OLAP 테이블에 로그 저장
- <img src="https://img.shields.io/badge/Grafana-F46800?style=for-the-badge&logo=grafana&logoColor=white"> 
    Grafana: ClickHouse 데이터 소스로 대시보드 시각화
- <img src="https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white">
    Docker / Docker Compose: 전체 개발 환경 오케스트레이션
- <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white">
    Python 3.10: 시뮬레이터, Watchdog 스크립트 등 보조 유틸
- <img src="https://img.shields.io/badge/Ubuntu-E95420?style=for-the-badge&logo=ubuntu&logoColor=white">
    Linux (Ubuntu 기반): VM 환경 및 파일 시스템 레이아웃

## 시스템 아키텍처

[이미지 추가 준비 중...]

- `log_gateway/generator.py` 는 서비스별 프로파일(트래픽 믹스, 시간대 가중치, 오류율 등)을 적용해 무한 스트림을 생성하고 Kafka 토픽(`logs.auth`, `logs.order`, ... 및 `logs.error`)으로 발행합니다.
- `spark_job/main.py` 는 Kafka 패턴 구독(`logs.*`) 후 `analytics.fact_log` 스키마로 파싱하여 ClickHouse에 append 합니다. 체크포인트는 `/data/spark_checkpoints`에 유지됩니다.
- `grafana/` 폴더의 프로비저닝 파일이 ClickHouse 데이터 소스를 등록하고, 저장한 대시보드로 실시간 지표를 확인합니다.
- `monitor/docker_watchdog.py` 는 필요 시 별도 터미널에서 실행하여 Kafka/Spark/ClickHouse/Grafana 컨테이너 이벤트 및 오류 로그를 웹훅으로 알릴 수 있는 경량 감시 도구입니다.

## 실행 방법
```bash
# 0. 사전 준비
# - Docker / Docker Compose 설치
# - VM이라면 /data 파티션을 미리 마운트하고 아래 디렉터리를 생성해 rw 권한을 부여한다.
#     sudo mkdir -p /data/kafka-logs /data/kafka-meta /data/spark_checkpoints \
#                  /home/kang/log-monitoring/data/clickhouse/{data,logs} \
#                  /home/kang/log-monitoring/data/grafana
#     sudo chown -R $USER:$USER /data /home/kang/log-monitoring/data
# - 방화벽/보안 그룹에서 29092(Kafka), 4040(Spark UI), 3000(Grafana) 등을 허용

# 1. Kafka + Kafka UI만 우선 기동 (토픽/메시지 확인용)
docker compose up -d kafka kafka-ui

# 2. Spark, ClickHouse, Grafana 파이프라인 기동
docker compose up -d spark clickhouse grafana spark

# 3. 로그 시뮬레이터 기동
docker compose up -d simulator

# 4. 상태 점검
docker compose ps
curl http://localhost:8000/ping                 # log_gateway FastAPI
curl http://localhost:4040/api/v1/applications  # Spark UI REST

# 5. (선택) CLI 모니터링
python monitor/docker_watchdog.py
```
추가 확인 포인트:
- Kafka UI(http://localhost:8080)에서 `logs.*` 토픽 메시지 수신 여부를 확인합니다.
- ClickHouse에 적재되는 데이터는 `docker exec -it clickhouse clickhouse-client --query "SELECT count() FROM analytics.fact_log"` 로 조회할 수 있습니다.
- Grafana(http://localhost:3000, 기본 계정 `admin` / `admin`)에서 ClickHouse 데이터 소스와 대시보드를 통해 지표를 확인합니다.

## 목표
- 서비스 로그 데이터 파이프라인의 종단 간 지연·신뢰성을 검증하고, 트래픽 시뮬레이터를 통해 다양한 부하 시나리오를 재현합니다.
- Spark → ClickHouse 적재 경로를 표준화하여 추후 실시간 탐지/알림 요건에 활용할 수 있는 분석 레이어를 정립합니다.
- Grafana를 기반으로 기본 운영 대시보드와 경량 Watchdog 스크립트를 활용한 장애 탐지 루틴을 수립합니다.
