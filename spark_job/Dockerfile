# -----------------------------------------------------------------------------
# 파일명 : spark_job/Dockerfile
# 목적   : bitnami/spark 기반 Spark Structured Streaming 잡 컨테이너
# 특징   :
#   - spark:python3 이미지 사용 (Spark + PySpark + Java 포함)
#   - 추가 pip 의존성 없음 (pyspark는 베이스 이미지에 포함)
#   - JAR 의존성은 spark_job/jars/에 사전 커밋하여 Maven 빌드 단계 제거
#   - python -m spark_job.main 으로 스트리밍 잡 실행
# -----------------------------------------------------------------------------

FROM spark:4.0.1-python3

USER root

ENV SPARK_VERSION=4.0.1
ENV SCALA_VERSION=2.13
ENV SPARK_HOME=/opt/spark

WORKDIR /app

RUN mkdir -p /opt/spark/jars/local

COPY common ./common
COPY spark_job ./spark_job
COPY spark_job/jars /opt/spark/jars/local

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH="/app"

# 비루트 유저
RUN useradd -m appuser && chown -R appuser:appuser /app
USER appuser

CMD ["python", "-m", "spark_job.main"]
