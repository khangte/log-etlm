#!/usr/bin/env python3
# 용도: Kafka 최신 오프셋과 Spark 체크포인트를 비교해 토픽별 lag를 계산한다.
from __future__ import annotations

import json
import logging
import re
import subprocess
import sys

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

TOPICS = ["logs.auth", "logs.order", "logs.payment", "logs.error"]
KAFKA_CONTAINER = "kafka"
SPARK_CONTAINER = "spark-driver"
CHECKPOINT_DIR = "/data/log-etlm/spark_checkpoints/fact_event/offsets"


def sh(cmd: list[str]) -> str:
    result = subprocess.run(cmd, text=True, capture_output=True, check=True)
    return result.stdout


def main() -> int:
    topics_str = " ".join(TOPICS)
    k_out = sh(
        [
            "docker",
            "exec",
            "-i",
            KAFKA_CONTAINER,
            "bash",
            "-lc",
            f"for t in {topics_str}; do "
            "kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic \"$t\" --time -1; "
            "done",
        ]
    )

    latest: dict[str, dict[int, int]] = {}
    for line in k_out.splitlines():
        m = re.match(r"(logs\.[^:]+):(\d+):(\d+)$", line.strip())
        if m:
            t, p, o = m.group(1), int(m.group(2)), int(m.group(3))
            latest.setdefault(t, {})[p] = o

    if not latest:
        logger.error("ERROR: kafka_latest 파싱 실패")
        logger.error("RAW_KAFKA_OUT:\n%s", k_out)
        return 1

    cp_file = (
        sh(
            [
                "docker",
                "exec",
                "-i",
                SPARK_CONTAINER,
                "bash",
                "-lc",
                f"ls -1 {CHECKPOINT_DIR} 2>/dev/null | sort -n | tail -n 1",
            ]
        )
        .strip()
    )
    if not cp_file:
        logger.error("ERROR: checkpoint 파일 없음: %s", CHECKPOINT_DIR)
        return 1
    if not cp_file.isdigit():
        logger.error("ERROR: 예상치 못한 checkpoint 파일명: %r", cp_file)
        return 1

    cp_text = sh(
        [
            "docker",
            "exec",
            "-i",
            SPARK_CONTAINER,
            "bash",
            "-lc",
            f"cat {CHECKPOINT_DIR}/{cp_file}",
        ]
    )
    lines = cp_text.splitlines()

    chk = None
    for line in reversed(lines):
        if line.startswith("{") and "logs." in line:
            chk = json.loads(line)
            break

    if not chk:
        logger.error("ERROR: checkpoint JSON 파싱 실패: %s", cp_file)
        logger.error("TAIL:\n%s", "\n".join(lines[-5:]))
        return 1

    total = 0
    logger.info("checkpoint_file: %s", cp_file)
    for topic in sorted(latest.keys()):
        parts = latest[topic]
        ct = chk.get(topic, {})
        for part in sorted(parts.keys()):
            latest_offset = parts[part]
            # 체크포인트 JSON의 파티션 키는 문자열·정수 양쪽으로 존재할 수 있어 양쪽 시도
            checkpoint_offset = int(ct.get(str(part), ct.get(part, 0)))
            lag = max(0, latest_offset - checkpoint_offset)
            total += lag
            logger.info(
                "%s p%s: checkpoint=%s latest=%s lag=%s",
                topic, part, checkpoint_offset, latest_offset, lag,
            )
    logger.info("TOTAL_LAG: %s", total)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
