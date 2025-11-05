# -----------------------------------------------------------------------------
# 파일명 : simulator/Dockerfile
# 목적   : log-forge 시뮬레이터를 컨테이너로 실행하기 위한 이미지 정의
# 특징   :
#   - Python 3.11 slim 기반
#   - tzdata 설치(ZoneInfo에서 Asia/Seoul 사용), librdkafka(옵션: confluent-kafka 사용 시)
#   - requirements.txt 설치 후, simulator 패키지/프로파일/템플릿 복사
#   - 기본 CMD는 baseline 프로파일로 dry-run 10초 (원하면 덮어쓰기)
# 사용   :
#   docker build -t log-forge-sim:latest -f simulator/Dockerfile .
#   docker run --rm log-forge-sim:latest
#   # 실제 Kafka 발행 모드(예시):
#   docker run --rm log-forge-sim:latest \
#     python -m simulator.main --profile simulator/profiles/baseline.yaml --duration 60
# -----------------------------------------------------------------------------

FROM python:3.11-slim AS runtime

# 시스템 패키지: tzdata(Asia/Seoul), librdkafka(옵션: confluent-kafka가 있다면 사용)
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    tzdata \
    librdkafka-dev \
  && rm -rf /var/lib/apt/lists/*

# 타임존 데이터 설정(컨테이너 내 KST 계산 안정성)
ENV TZ=Asia/Seoul

# 작업 디렉터리
WORKDIR /app

# 파이썬 의존성 먼저 설치(레이어 캐시 효율)
COPY simulator/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# 애플리케이션 복사
# 패키지 코드
COPY simulator /app/simulator
# (필요 시) 루트 기타 파일이 있으면 추가 COPY 라인으로 붙이면 됨

# 비루트 권장 (선택)
RUN useradd -m appuser && chown -R appuser:appuser /app
USER appuser

# 기본 실행(드라이런 10초): 실제 사용 시 "커맨드 오버라이드" 권장
CMD ["python", "-m", "simulator.main", "--dry-run", "--duration", "10"]
