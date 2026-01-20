# -----------------------------------------------------------------------------
# 파일명 : spark_job/Dockerfile
# 목적   : bitnami/spark 기반 Spark Structured Streaming 잡 컨테이너
# 특징   :
#   - spark:python3 이미지 사용 (Spark + PySpark + Java 포함)
#   - 추가 파이썬 의존성(clickhouse-driver 등)만 pip로 설치
#   - python -m spark_job.main 으로 스트리밍 잡 실행
# -----------------------------------------------------------------------------

FROM spark:4.0.1-python3

USER root

ENV SPARK_VERSION=4.0.1
ENV SCALA_VERSION=2.13
ENV SPARK_HOME=/opt/spark

WORKDIR /app

# 필요하면 curl, ping 같은 유틸만 남겨도 됨 (옵션)
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip maven ca-certificates \
    && ln -sf /usr/bin/python3 /usr/bin/python \
    && rm -rf /var/lib/apt/lists/*

# 2) 파이썬 의존성 설치 (pyspark는 이미 이미지에 있으니 굳이 안 넣어도 됨)
COPY spark_job/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r ./requirements.txt

# 3) Spark 런타임 JAR 의존성 사전 다운로드
RUN mkdir -p /opt/spark/jars/extra /opt/spark/jars/local
RUN cat > /tmp/spark-deps-pom.xml <<'EOF'
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>log-etlm</groupId>
  <artifactId>spark-deps</artifactId>
  <version>1.0.0</version>
  <dependencies>
    <dependency>
      <groupId>org.apache.spark</groupId>
      <artifactId>spark-sql-kafka-0-10_2.13</artifactId>
      <version>4.0.1</version>
    </dependency>
    <dependency>
      <groupId>org.apache.spark</groupId>
      <artifactId>spark-token-provider-kafka-0-10_2.13</artifactId>
      <version>4.0.1</version>
    </dependency>
    <dependency>
      <groupId>com.clickhouse</groupId>
      <artifactId>clickhouse-jdbc</artifactId>
      <version>0.4.6</version>
    </dependency>
  </dependencies>
</project>
EOF
RUN mvn -q -f /tmp/spark-deps-pom.xml dependency:copy-dependencies \
    -DincludeScope=runtime \
    -DoutputDirectory=/opt/spark/jars/extra \
    -DskipTests
RUN rm -rf /root/.m2 /tmp/spark-deps-pom.xml

# 4) 애플리케이션 코드
COPY common ./common
COPY spark_job ./spark_job
COPY spark_job/jars /opt/spark/jars/local

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH="/app"

# 5) 비루트 유저
RUN useradd -m appuser && chown -R appuser:appuser /app
USER appuser

CMD ["python", "-m", "spark_job.main"]
