#!/usr/bin/env python3
# 용도: Kafka 최신 오프셋과 Spark 체크포인트를 비교해 토픽별 lag를 계산한다.
from __future__ import annotations

import json
import re
import subprocess
import sys


TOPICS = ["logs.auth", "logs.order", "logs.payment", "logs.error"]
KAFKA_CONTAINER = "kafka"
SPARK_CONTAINER = "spark-driver"
CHECKPOINT_DIR = "/data/log-etlm/spark_checkpoints/fact_event/offsets"


def sh(cmd: list[str]) -> str:
    return subprocess.check_output(cmd, text=True)


def main() -> int:
    k_out = sh(
        [
            "docker",
            "exec",
            "-i",
            KAFKA_CONTAINER,
            "bash",
            "-lc",
            "for t in logs.auth logs.order logs.payment logs.error; do "
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
        print("ERROR: kafka_latest 파싱 실패")
        print("RAW_KAFKA_OUT:\n", k_out)
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
        print(f"ERROR: checkpoint 파일 없음: {CHECKPOINT_DIR}")
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
        print(f"ERROR: checkpoint JSON 파싱 실패: {cp_file}")
        print("TAIL:\n", "\n".join(lines[-5:]))
        return 1

    total = 0
    print("checkpoint_file:", cp_file)
    for topic in sorted(latest.keys()):
        parts = latest[topic]
        ct = chk.get(topic, {})
        for part in sorted(parts.keys()):
            latest_offset = parts[part]
            checkpoint_offset = int(ct.get(str(part), ct.get(part, 0)))
            lag = max(0, latest_offset - checkpoint_offset)
            total += lag
            print(
                f"{topic} p{part}: checkpoint={checkpoint_offset} "
                f"latest={latest_offset} lag={lag}"
            )
    print("TOTAL_LAG:", total)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
