# Docker Watchdog

경량 Python 스크립트 `docker_watchdog.py` 로 Kafka/Spark/ClickHouse 컨테이너 상태를 실시간 감시합니다. Prometheus 없이도 빠르게 알림을 받을 수 있도록 설계됐습니다.

## 실행 방법
```bash
cd /home/kang/log-monitoring
chmod 774 ./monitor/.env
set -a && source monitor/.env && set +a
python3 monitor/docker_watchdog.py
```

- `ALERT_WEBHOOK_URL` 을 지정하면 Slack 등 Webhook 으로 알림을 보냅니다. 미지정 시 표준 출력 로그만 남깁니다.
- 스크립트는 `docker` CLI 에 의존하므로 호스트에서 `docker` 명령이 가능해야 합니다.

## 모니터링 항목
- **컨테이너 이벤트**: `docker events` 스트림을 읽어 kafka/spark/clickhouse 가 `die`, `health_status: unhealthy` 상태가 되면 즉시 경고합니다.
- **healthcheck**: 30초마다 `docker inspect` 로 Health 상태를 확인(`docker-compose.yml`의 spark/clickhouse healthcheck 설정 활용).
- **로그 패턴**: `docker logs -f` 로 각 컨테이너 로그를 추적해 `OutOfMemoryError`, `StreamingQueryException`, `Code: 241` 등의 문자열을 감지하면 경보를 보냅니다.
- **Spark REST**: `http://localhost:4040/api/v1/applications` 를 주기적으로 호출하여 UI 가 응답하지 않으면 알림을 전송합니다.

## 서비스로 상시 실행하기 (예시)
`systemd` 를 사용하는 경우 `/etc/systemd/system/logmonitoring-watchdog.service` 파일을 만들어 항상 자동 재시작되도록 할 수 있습니다.

```
[Unit]
Description=LogMonitoring Docker Watchdog
After=docker.service

[Service]
WorkingDirectory=/home/kang/log-monitoring
Environment=ALERT_WEBHOOK_URL=https://hooks.slack.com/services/XXX/YYY/ZZZ
ExecStart=/usr/bin/python3 monitor/docker_watchdog.py
Restart=always

[Install]
WantedBy=multi-user.target
```

파일을 추가한 뒤 `sudo systemctl daemon-reload && sudo systemctl enable --now logmonitoring-watchdog` 으로 활성화하면 시스템 부팅 시 자동으로 실행됩니다.
