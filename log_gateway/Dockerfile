# -----------------------------------------------------------------------------
# 파일명 : log_gateway/Dockerfile
# 목적   : log_gateway FastAPI 시뮬레이터(uvicorn + generator)를 컨테이너로 실행하는 이미지 정의
# 특징   :
#   - Python 3.11 slim 기반
#   - tzdata 설치(ZoneInfo에서 Asia/Seoul 사용), librdkafka(옵션: confluent-kafka 사용 시)
#   - requirements 설치 후 log_gateway 패키지를 복사해 FastAPI 앱/시뮬레이터 포함
#   - 기본 CMD는 uvicorn log_gateway.main:app
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
COPY log_gateway/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# 애플리케이션 복사
# 패키지 코드
COPY log_gateway /app/log_gateway
# (필요 시) 루트 기타 파일이 있으면 추가 COPY 라인으로 붙이면 됨

# 비루트 권장 (선택)
RUN useradd -m appuser && chown -R appuser:appuser /app
USER appuser

# 기본 실행(드라이런 10초): 실제 사용 시 "커맨드 오버라이드" 권장
CMD ["uvicorn", "log_gateway.main:app", "--host", "0.0.0.0", "--port", "8000"]
