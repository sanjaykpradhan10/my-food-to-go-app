# Session state — 2026-08-10

Purpose: restore full context for continuing the FTGO Ch.11 observability work in a new session. Read this file first if resuming.

## Where things stand

| Sub-project | Status |
|---|---|
| §11.1–§11.3.4 | Done (see prior session-state files) |
| §11.3.5 Exception tracking | **Done — PR #31, merged** |
| §11.3.6 Audit logging | Not started — next item |

**Next action when resuming:** run the brainstorm → spec → plan → SDD pipeline for §11.3.6 Audit logging, the last unstarted Ch.11 sub-project. Once it ships, Ch.11 as a whole flips to Done, triggering the CLAUDE.md full chapter-completion documentation sweep (ARCHITECTURE.md full sections w/ diagrams for every pattern, every touched service README gets full parity, CONTEXT.md's "Concept understanding" section updated).

## What just happened (this session)

Executed the full pipeline for **Ch.11 §11.3.5 Exception tracking**, using self-hosted GlitchTip (Sentry-protocol-compatible) instead of full self-hosted Sentry (verified too heavy — needs ClickHouse+Kafka+Redis+Postgres) or Sentry SaaS (breaks self-hosted convention).

1. Spec: `docs/superpowers/specs/2026-08-10-exception-tracking-design.md`. Plan: `docs/superpowers/plans/2026-08-10-exception-tracking.md`. Both committed to `main`.
2. SDD in worktree `ch11-exception-tracking` (branch `worktree-ch11-exception-tracking`), 4 tasks:
   - Task 1: GlitchTip stack (`glitchtip`, `glitchtip-db`, `glitchtip-redis`) + one-shot `glitchtip-provisioner` writing DSN to a shared `sentry-dsn` Docker volume. Fix round: DSN host was `localhost` (unreachable cross-container) → rewritten to `glitchtip`.
   - Task 2: `sentry-spring-boot-starter` added to all 9 `actuatorModules` services, DSN sourced via entrypoint wrapper script reading the shared volume at container startup. Clean review.
   - Task 3: ADMIN-gated diagnostic endpoint (`GET /orders/_diagnostics/trigger-exception`, throws uncaught `IllegalStateException`) + Cucumber scenario polling GlitchTip's issues API. **Fix round: a live GlitchTip API token was hardcoded as a fallback in test source — revoked and removed; provisioning now mints and persists the token to the same gitignored `dsn.env` file as the DSN.**
   - Task 4: doc sync (README.md, CONTEXT.md, docs/ARCHITECTURE.md new section) — per-change sync only, did not trigger the chapter sweep (§11.3.6 still unstarted).
3. **Final whole-branch review (Opus)** found 1 Critical + 2 Important:
   - Critical: `sentry-spring-boot-starter` (no `-jakarta` suffix) is Sentry's Spring Boot 2/javax variant — its `spring.factories` auto-config never loads on this Spring Boot 3/jakarta project, so the entire exception-capture pipeline was silently inert. Fixed by switching to `sentry-spring-boot-starter-jakarta:7.22.5`, and — critically — **actually verified end-to-end this time**: real docker-compose stack, real diagnostic-endpoint hit, real captured GlitchTip issue, real passing Cucumber run. Along the way discovered and added a missing `glitchtip-worker` Celery service, without which no event could ever have become a visible Issue (meaning nothing prior had ever really been verified).
   - Important: docs (ARCHITECTURE.md, README.md, CONTEXT.md, a build.gradle comment) falsely claimed traceId/spanId correlation with Tempo/Kibana was implemented — nothing in the dependency set actually wires that up. Fixed by correcting the docs to state it's NOT implemented, rather than adding the real OTel integration (out of scope for the single allowed fix wave).
   - Important: all 9 Dockerfiles' entrypoints exported the entire shared `dsn.env` file (now 2 lines: DSN + API token) into every service container's environment, leaking an org-wide GlitchTip credential into services that don't need it. Fixed to export only the DSN line.
   - Bonus fix (found while verifying live): pre-existing duplicate `saga:` top-level key in `ftgo-order-service/application.yml` (issue #30, flagged but not fixed in the prior session) — SnakeYAML was silently discarding the first occurrence, blocking order-service from starting cleanly. Fixed in this same branch since it blocked live verification.
4. **One fix dispatch + one scoped re-review** confirmed all 3 findings addressed, verification claims well-evidenced (concrete `/proc/1/environ` checks, GlitchTip access logs, JUnit XML result), the 2 bonus fixes narrowly scoped and correct, no new breakage.
5. Deleted the SDD workspace, ran `finishing-a-development-branch`: `./gradlew compileJava compileTestJava` clean across all modules, user chose **push + PR**, PR #31 opened, then user asked to merge + clean up — merged and worktree/branch removed.

## Reusable context for the next sub-project

- **Book**: *Microservices Patterns* (Chris Richardson). PDF location saved in memory (`reference_book_pdf_location.md`).
- **Workflow**: brainstorm → spec → plan → SDD in isolated worktree → final whole-branch review (most capable model) → `finishing-a-development-branch` → PR → merge → cleanup. Run by default per `feedback_workflow_ftgo.md` memory.
- **Hard-learned lesson this session: task-scoped reviews miss cross-task and "does this actually run" defects.** Task 2's review approved `sentry-spring-boot-starter` as a reasonable version-pin deviation without checking Spring Boot 2-vs-3 artifact compatibility, and Task 3's e2e scenario was never actually executed end-to-end during its own task review (judged "too heavy" for the implementer's environment) — both defects were invisible until the final whole-branch review actually brought up the stack and ran the real thing. **For future Ch.11 sub-projects with a similar shape (new external dependency + an e2e verification scenario), budget for a real live end-to-end run at least once before calling the branch done — a scenario that was never executed provides no evidence the feature works, no matter how clean the code reads statically.**
- **Also learned: an implementer resuming a fix round can accidentally introduce a fresh secret leak while fixing something else** (Task 3's fix for the culprit-match issue is what led it to hand-mint and hardcode a token). When a fix round touches auth/credentials, explicitly re-flag "no hardcoded secrets" as a check in that round's dispatch, not just the original one.
- **`EnterWorktree` branches from `origin/main`, not local `main`** — merge local commits in manually if needed.
- **Model tiering**: haiku for scoped re-reviews of small fix diffs; sonnet for implementation/integration reviews; opus for the final whole-branch review only.
- **Known status**: issue #30 (order-service duplicate `saga:` key) — fixed via PR #31, no longer outstanding.
