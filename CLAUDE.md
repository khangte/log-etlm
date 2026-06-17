# CLAUDE.md

**Tradeoff:** These guidelines bias toward caution over speed. For trivial tasks, use judgment.

## 1. Engineering Principles

### 1.1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:

- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

### 1.2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

### 1.3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:

- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:

- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

### 1.4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:

- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:

```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

## 3. Project Rules

### 3.1. Environment Variables

<!-- - NEVER read or modify `.env`
- Modify `.env.example` only -->

- 환경변수 우선순위:
  - @.env
  - @config/env/
  - @docker-compose.yml

### 3.2. Cleanup

- Remove temporary and debug files before finishing
- NEVER create: `temp_*`, `*_new`, `*_old`, `*_backup`

### 3.3. Execution

- Local: `uv`
- Docker: `pip + requirements.txt`

### 3.4. Code

- Use type hints
- Use `logging`, never `print()`
- Add Korean docstrings to Python functions and classes
- Never use Chinese characters: write "분석", not "분析" or "分析"

### 3.5. Commits

- Commit only after tests pass
- Keep commits atomic
- Write commit messages in Korean
- **Always split commits by change type. Never bundle multiple concerns into a single commit.**
  - Split criteria:
    1. Internal logic changes (feat/refactor) — separate by file or feature unit
    2. Wiring/entrypoint changes (main, router registration, etc.) — separate commit
    3. Documentation changes (docs) — always separate from code commits

### 3.6. Naming

- Use `snake_case` for files, folders, and functions
- Use `PascalCase` for classes
- Wrap file paths in backticks
