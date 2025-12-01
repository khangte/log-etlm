# Docker Compose 상황별 명령 순서

## 1. 최초 환경 세팅
1. 의존 이미지 최신화  
   `docker compose pull`
2. 서비스 전부 빌드 (코드/의존성 캐시 준비)  
   `docker compose build`
3. 전체 스택 기동  
   `docker compose up -d`
4. 상태/로그 확인  
   `docker compose ps`  
   `docker compose logs -f`

## 2. 코드 수정 후 재배포
1. 수정 서비스만 재빌드  
   `docker compose build <service>`
2. 동일 서비스만 재시작(+재생성)  
   `docker compose up -d <service>`
3. 배포 확인 용 로그 팔로우  
   `docker compose logs -f <service>`

> 여러 서비스 변경 시 `docker compose build web spark_job` 처럼 공백으로 나열하고, 이후 `docker compose up -d web spark_job`을 실행합니다.

## 3. 컨테이너를 내렸다가 다시 올릴 때
1. 안전하게 중지  
   `docker compose stop` 또는 `docker compose stop <service>`
2. 컨테이너만 제거(이미지/볼륨 유지)  
   `docker compose rm -f` 또는 `docker compose rm -f <service>`
3. 재실행  
   `docker compose up -d` (또는 특정 서비스)
4. 정상 동작 확인  
   `docker compose ps`, 필요 시 `docker compose logs --tail=200 <service>`

## 4. 깨끗한 재시작(네트워크/볼륨까지 정리)
1. 전체 종료  
   `docker compose down`
2. 데이터 볼륨까지 제거가 필요하면  
   `docker compose down -v`
3. 이미지까지 초기화가 필요하면  
   `docker compose down --rmi all`
4. 이후 다시 **1. 최초 환경 세팅** 절차를 실행

## 5. 트러블슈팅/운영 중 자주 쓰는 흐름
- 실시간 로그 모니터링: `docker compose logs -f <service>`
- 컨테이너 쉘 진입: `docker compose exec <service> /bin/bash`
- 리빌드 없이 재시작(환경 변수 반영 등): `docker compose restart <service>`
- 포트/리소스 확인: `docker compose port <service> <container_port>`, `docker compose top`
- 이벤트 흐름 확인: `docker compose events`

필요 시 `docker compose -f docker-compose.yml -f docker-compose.override.yml ...` 식의 옵션을 각 단계에 붙여 같은 순서를 적용하면 됩니다.