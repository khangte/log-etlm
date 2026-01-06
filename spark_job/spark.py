from __future__ import annotations

from typing import Sequence

from pyspark.sql import SparkSession


def _build_spark_session(
    *,
    app_name: str,
    master: str | None,
    packages: Sequence[str],
    ui_port: str,
    event_log_enabled: bool,
    event_log_dir: str | None,
) -> SparkSession:
    builder = SparkSession.builder.appName(app_name)
    if master:
        builder = builder.master(master)
    if packages:
        builder = builder.config("spark.jars.packages", ",".join(packages))

    builder = (
        builder.config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.ui.enabled", "true")
        .config("spark.ui.port", ui_port)
    )

    if event_log_enabled:
        builder = builder.config("spark.eventLog.enabled", "true")
        if event_log_dir:
            builder = builder.config("spark.eventLog.dir", event_log_dir)

    return builder.getOrCreate()


def build_streaming_spark(
    *,
    master: str | None,
    app_name: str = "LogForge_Spark_Job",
) -> SparkSession:
    packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1",
        "com.clickhouse:clickhouse-jdbc:0.4.6",
    ]
    return _build_spark_session(
        app_name=app_name,
        master=master,
        packages=packages,
        ui_port="4040",
        event_log_enabled=True,
        event_log_dir="/data/log-etlm/spark-events",
    )


def build_batch_spark(
    *,
    app_name: str = "LogForge_Dim_Batch",
    master: str = "local[*]",
) -> SparkSession:
    packages = [
        "com.clickhouse:clickhouse-jdbc:0.4.6",
    ]
    return _build_spark_session(
        app_name=app_name,
        master=master,
        packages=packages,
        ui_port="4041",
        event_log_enabled=False,
        event_log_dir=None,
    )
