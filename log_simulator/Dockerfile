# -----------------------------------------------------------------------------
# 파일명 : log_simulator/Dockerfile
# 목적   : log_simulator FastAPI 시뮬레이터(uvicorn + generator)를 컨테이너로 실행하는 이미지 정의
# 특징   :
#   - Python 3.11 slim 기반
#   - tzdata 설치(ZoneInfo에서 Asia/Seoul 사용)
#   - requirements 설치 후 log_simulator 패키지를 복사해 FastAPI 앱/시뮬레이터 포함
#   - 기본 CMD는 uvicorn log_simulator.main:app
# -----------------------------------------------------------------------------

FROM python:3.11-slim AS runtime

# 시스템 패키지: tzdata(Asia/Seoul)
RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

# 타임존 데이터 설정(컨테이너 내 KST 계산 안정성)
ENV TZ=Asia/Seoul
ENV PYTHONUNBUFFERED=1

# 작업 디렉터리
WORKDIR /app

# 파이썬 의존성 먼저 설치(레이어 캐시 효율)
COPY log_simulator/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r ./requirements.txt

# 애플리케이션 복사
# 패키지 코드
COPY log_simulator ./log_simulator

# 비루트 권장 (선택)
RUN useradd -m appuser && chown -R appuser:appuser /app
USER appuser
