# ================================
# Docker Compose 명령어 치트시트
# ================================
# 전제: Docker Compose V2 (`docker compose …`)
# 참고: <>는 사용자 값으로 교체

# --------------------------------
# 기본 실행/중지/정리
# --------------------------------
# 백그라운드 실행(필요 시 빌드)
docker compose up -d

# 강제 재생성 + 고아 컨테이너 제거
docker compose up -d --force-recreate --remove-orphans

# 특정 서비스만 올리기
docker compose up -d <service1> <service2>

# 종료(컨테이너/네트워크 제거)
docker compose down

# 종료 + 볼륨까지 제거(주의)
docker compose down -v

# 종료 + 이미지까지 제거(주의)
docker compose down --rmi all

# --------------------------------
# 빌드/이미지
# --------------------------------
# 이미지 빌드
docker compose build

# 캐시 무시 빌드
docker compose build --no-cache

# 특정 서비스만 빌드
docker compose build <service>

# up 시 자동 빌드
docker compose up -d --build

# 레지스트리에서 이미지 받기
docker compose pull

# 특정 서비스만 pull
docker compose pull <service>

# 이미지 푸시(레지스트리로)
docker compose push
docker compose push <service>

# 현재 프로젝트에서 사용하는 이미지 목록
docker compose images

# --------------------------------
# 상태/모니터링/로그
# --------------------------------
# 서비스 상태
docker compose ps

# 서비스별 프로세스(top)
docker compose top

# 전체 로그 팔로우
docker compose logs -f

# 특정 서비스 로그
docker compose logs -f <service>

# 최근 N줄만
docker compose logs --tail=200 <service>

# 이벤트 스트림
docker compose events

# 포트 확인
docker compose port <service> <container_port>

# --------------------------------
# 컨테이너 조작
# --------------------------------
# 쉘 접속(인터랙티브)
docker compose exec <service> /bin/sh
docker compose exec <service> /bin/bash

# TTY 없이 명령 실행(CI 등)
docker compose exec -T <service> <cmd>

# 일회성 컨테이너로 커맨드 실행(서비스 컨텍스트로)
docker compose run --rm <service> <cmd>

# 시작/중지/재시작
docker compose start
docker compose stop
docker compose restart
docker compose restart <service>

# 일시정지/해제
docker compose pause
docker compose unpause

# 강제 종료(KILL)
docker compose kill
docker compose kill <service>

# 컨테이너만 제거(중지 상태)
docker compose rm -f
docker compose rm -f <service>

# 컨테이너만 생성(실행은 안 함)
docker compose create
docker compose create <service>

# --------------------------------
# 설정/프로젝트 관리
# --------------------------------
# 실제 적용될 Compose 구성 출력(병합 결과)
docker compose config
docker compose config --services   # 서비스 이름만
docker compose config --profiles   # 프로필만

# Compose 파일 검증만
docker compose config --quiet

# 현재 디렉터리의 Compose 프로젝트 목록
docker compose ls

# 버전 정보
docker compose version

# --------------------------------
# 고급 옵션 예시
# --------------------------------
# 특정 Compose 파일(복수 가능)
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d

# 프로젝트 이름 지정(디렉터리명 대신)
docker compose -p myproject up -d

# .env 대체
docker compose --env-file .env.local up -d

# 스케일 아웃(서비스 복제 수)
docker compose up -d --scale <service>=3

# 프로필 사용(Compose 파일 내 profiles 키)
docker compose --profile <name> up -d

# 빌드 인자/타깃 스테이지
docker compose build --build-arg KEY=VALUE --target production <service>

# CPU/메모리 제한(서비스 override 예시로 많이 사용)
# -> docker-compose.override.yml 에 넣고 사용 권장

# --------------------------------
# 파일/데이터 조작
# --------------------------------
# 컨테이너↔호스트 파일 복사
docker compose cp <service>:/path/in/container /path/on/host
docker compose cp /path/on/host <service>:/path/in/container

# 볼륨 정리(사용하지 않는 로컬 볼륨)
docker volume prune

# 네트워크 정리(사용하지 않는 네트워크)
docker network prune

# --------------------------------
# 트러블슈팅 팁
# --------------------------------
# 특정 서비스만 재빌드 후 재시작
docker compose build <service> && docker compose up -d <service>

# 로그에서 최근 오류만 빠르게 보기
docker compose logs --tail=200 <service>

# 컨테이너 내 프로세스/환경 확인
docker compose exec <service> ps aux
docker compose exec <service> env

# 포트 충돌 확인(호스트)
# macOS/Linux
lsof -i :<port>
# Windows (PowerShell)
netstat -ano | findstr :<port>
