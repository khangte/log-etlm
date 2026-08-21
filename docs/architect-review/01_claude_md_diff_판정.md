# CLAUDE.md 변경 건 판정 (refactor/260821)

- 제기: reviewer (브랜치 diff 검토 중 발견)
- 판정일: 2026-08-21
- 결론: **범위 이탈 아님. developer 무관.** 팀 셋업 스크립트(`ai-setup:team`)의 자동 주입이다.

## 확인한 증거

1. **커밋에 포함되지 않았다.** `git diff --stat main...HEAD -- CLAUDE.md` 결과가 비어 있다.
   즉 이번 브랜치의 어떤 커밋도 CLAUDE.md를 건드리지 않았고, 변경은 워킹트리 미스테이지 상태로만
   존재한다(`git status` = ` M CLAUDE.md`). reviewer가 본 것은 커밋 diff가 아니라 워킹트리 diff다.
2. **CLAUDE.md의 마지막 커밋은 a202226 "chore: CLAUDE.md를 버전 관리에 포함"(2026-06-17)** 하나뿐이며,
   이번 리팩터링 커밋들(ea2b1cf, 83984e3 등)과 두 달 떨어져 있다.
3. **HEAD 버전에는 `ai-setup:team` 마커가 없다.** `git show HEAD:CLAUDE.md | grep ai-setup` 결과가 비었다.
   워킹트리 버전에만 `<!-- ai-setup:team:start -->` / `<!-- ai-setup:team:end -->` 블록이 있다.
4. **타임스탬프가 셋업 실행 시점과 일치한다.** CLAUDE.md mtime 2026-08-20 23:48:24,
   `.team/_runtime/architect.prompt.md` mtime 23:48:06 — 18초 간격의 같은 배치다.
   역할 런타임 파일 생성과 CLAUDE.md 편집이 한 실행에서 이뤄졌다.
5. **어느 파인도 CLAUDE.md를 쓰지 않았다.** `.claude-logs/*.jsonl` 네 개 모두에서
   CLAUDE.md를 대상으로 한 파일 쓰기 툴 호출이 0건이다(developer 포함).

## 삭제된 섹션에 대한 판단

기존 `1. Engineering Principles`(Think Before Coding / Simplicity First / Surgical Changes /
Goal-Driven Execution)와 `3. Project Rules`의 Cleanup / Execution / Code / Commits / Naming이
함께 사라진 것은 셋업 스크립트가 파일을 부분 삽입이 아니라 재작성하는 방식이기 때문이다.
developer의 delete 작업과 인과관계가 없다.

다만 결과적으로 **팀 공통 규칙이 소실된 상태**다. 특히 커밋 분리 규칙(3.5)은 현재도 유효한
운용 규칙이므로 복구가 필요하다. 이는 이번 리팩터링 범위와 별개의 후속 작업으로 분리한다.

## 조치

- developer에 대한 수정 지시 없음 (이탈 아님).
- 후속: 삭제된 Engineering Principles / Project Rules 섹션을 `ai-setup:team` 블록 **바깥**에
  복원할지 여부를 lead가 결정한다. 블록 안에 넣으면 다음 셋업 실행 때 다시 덮인다.
- 이번 브랜치 커밋에는 CLAUDE.md가 없으므로, 리팩터링 커밋에 섞이지 않도록 별도 커밋으로 처리한다.
