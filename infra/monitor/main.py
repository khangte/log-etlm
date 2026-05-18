#!/usr/bin/env python3
"""
경량 Docker 모니터링 스크립트.
- kafka / spark-driver / clickhouse 컨테이너 이벤트 및 로그를 감시
- ClickHouse 기반 단계별 지표를 점검해 이상 시 알림
- OOM/StreamingQueryException 등 특정 키워드 또는 health 상태 변경 시 Slack/webhook 알림
- Prometheus 없이 빠르게 붙일 수 있는 최소 방어선
"""

from __future__ import annotations

import asyncio

from watchdog.clickhouse_probe import clickhouse_stage_probe
from watchdog.config import LOG_PATTERNS
from watchdog.docker_probes import check_health_loop, watch_events, watch_logs
from watchdog.service_probes import grafana_health_probe, spark_rest_probe


async def main() -> None:
    tasks = [
        asyncio.create_task(watch_events()),
        asyncio.create_task(check_health_loop()),
        asyncio.create_task(spark_rest_probe()),
        asyncio.create_task(grafana_health_probe()),
        asyncio.create_task(clickhouse_stage_probe()),
    ]
    for container, pats in LOG_PATTERNS.items():
        tasks.append(asyncio.create_task(watch_logs(container, pats)))

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("watchdog stopped.")
