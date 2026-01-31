from __future__ import annotations

import os
from pathlib import Path
from typing import Sequence

from pyspark import SparkContext
from pyspark.sql import SparkSession

from common.get_env import get_env_int, get_env_str


def _reset_stopped_spark_context() -> None:
    """중단된 SparkContext 참조를 정리한다."""
    active_sc = getattr(SparkContext, "_active_spark_context", None)
    if not active_sc:
        return
    jsc = getattr(active_sc, "_jsc", None)
    if jsc is None:
        return
    try:
        if jsc.sc().isStopped():
            SparkContext._active_spark_context = None
            if hasattr(SparkSession, "_instantiatedSession"):
                SparkSession._instantiatedSession = None
            if hasattr(SparkSession, "_activeSession"):
                SparkSession._activeSession = None
    except Exception:
        return


def _build_spark_session(
    *,
    app_name: str,
    master: str | None,
    packages: Sequence[str],
    extra_jars: Sequence[str],
    ui_port: str,
    event_log_enabled: bool,
    event_log_dir: str | None,
    shuffle_partitions: int | None,
    driver_memory: str | None,
    executor_memory: str | None,
) -> SparkSession:
    """SparkSession을 생성한다."""
    _reset_stopped_spark_context()
    builder = SparkSession.builder.appName(app_name)
    if master:
        builder = builder.master(master)
    if extra_jars:
        builder = builder.config("spark.jars", ",".join(extra_jars))
    elif packages:
        builder = builder.config("spark.jars.packages", ",".join(packages))

    builder = (
        builder.config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.ui.enabled", "true")
        .config("spark.ui.port", ui_port)
        .config("spark.sql.streaming.ui.enabled", "true")
    )
    if shuffle_partitions is not None:
        builder = builder.config("spark.sql.shuffle.partitions", str(shuffle_partitions))
    if driver_memory:
        builder = builder.config("spark.driver.memory", driver_memory)
    if executor_memory:
        builder = builder.config("spark.executor.memory", executor_memory)

    if event_log_enabled:
        builder = builder.config("spark.eventLog.enabled", "true")
        if event_log_dir:
            builder = builder.config("spark.eventLog.dir", event_log_dir)

    return builder.getOrCreate()


def _resolve_jar_dir(
    jar_dir: Path,
    required_names: Sequence[str],
) -> list[str]:
    """필수 JAR이 존재하면 디렉터리의 모든 JAR을 반환한다."""
    if not jar_dir.is_dir():
        return []
    for name in required_names:
        if not (jar_dir / name).is_file():
            return []
    return [str(path) for path in sorted(jar_dir.glob("*.jar"))]


def build_streaming_spark(
    *,
    master: str | None,
    app_name: str = "LogForge_Spark_Job",
) -> SparkSession:
    """스트리밍용 SparkSession을 생성한다."""
    env = os.environ
    packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1",
        "com.clickhouse:clickhouse-jdbc:0.4.6",
    ]
    required_streaming_jars = [
        "spark-sql-kafka-0-10_2.13-4.0.1.jar",
        "spark-token-provider-kafka-0-10_2.13-4.0.1.jar",
        "clickhouse-jdbc-0.4.6.jar",
    ]
    extra_jars = _resolve_jar_dir(Path("/opt/spark/jars/extra"), required_streaming_jars)
    if not extra_jars:
        extra_jars = _resolve_jar_dir(
            Path("/opt/spark/jars/local"),
            required_streaming_jars,
        )
    driver_memory = get_env_str(env, "SPARK_STREAM_DRIVER_MEMORY") or None
    executor_memory = get_env_str(env, "SPARK_STREAM_EXECUTOR_MEMORY") or None
    shuffle_partitions = get_env_int(env, "SPARK_STREAM_SHUFFLE_PARTITIONS")
    return _build_spark_session(
        app_name=app_name,
        master=master,
        packages=packages,
        extra_jars=extra_jars,
        ui_port="4040",
        event_log_enabled=True,
        event_log_dir="/data/log-etlm/spark-events",
        shuffle_partitions=shuffle_partitions or 8,
        driver_memory=driver_memory,
        executor_memory=executor_memory,
    )


def build_batch_spark(
    *,
    app_name: str = "LogForge_Dim_Batch",
    master: str | None = None,
) -> SparkSession:
    """배치용 SparkSession을 생성한다."""
    env = os.environ
    resolved_master = master or get_env_str(env, "SPARK_BATCH_MASTER") or "local[*]"
    shuffle_partitions = get_env_int(env, "SPARK_BATCH_SHUFFLE_PARTITIONS")
    driver_memory = get_env_str(env, "SPARK_BATCH_DRIVER_MEMORY")
    executor_memory = get_env_str(env, "SPARK_BATCH_EXECUTOR_MEMORY")
    packages = [
        "com.clickhouse:clickhouse-jdbc:0.4.6",
    ]
    extra_jars = _resolve_jar_dir(
        Path("/opt/spark/jars/extra"),
        ["clickhouse-jdbc-0.4.6.jar"],
    )
    if not extra_jars:
        extra_jars = _resolve_jar_dir(
            Path("/opt/spark/jars/local"),
            ["clickhouse-jdbc-0.4.6.jar"],
        )
    return _build_spark_session(
        app_name=app_name,
        master=resolved_master,
        packages=packages,
        extra_jars=extra_jars,
        ui_port="4041",
        event_log_enabled=False,
        event_log_dir=None,
        shuffle_partitions=shuffle_partitions or 8,
        driver_memory=driver_memory,
        executor_memory=executor_memory,
    )
