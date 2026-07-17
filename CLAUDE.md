# CLAUDE.md

Project-specific instructions for Claude Code when working in this repository. See also `CONTEXT.md` (learning progress/session log) and `README.md` (project overview).

## Documentation stays in sync with code

Whenever a change adds, removes, or alters behavior (a new service, a new event, a new pattern, a schema change, a config flag), update the relevant documentation in the same change:

- `README.md` — service list/status, tech stack, architecture, "Book progress" table.
- `CONTEXT.md` — "Current position", "Services to build" table, "Patterns reference" checklist, session log entry.
- Any per-topic doc under `docs/` that the change touches (e.g. an architecture or event-catalog doc, if one exists for the area being changed).

Documentation updates are not a separate follow-up task — they land in the same commit/PR as the code change they describe, following the existing convention already used for Ch.1–4 sessions (see `docs/session-*.md` and the "Session log" entries in `CONTEXT.md`).

## Code comments

Write comments that explain *why*, not *what* — the code already says what it does. Add a comment when there's a non-obvious constraint, a workaround for a specific bug or library quirk, or a design decision a future reader would otherwise have to reverse-engineer (e.g. why a Kafka topic carries a particular event shape, why a threshold value was chosen, why a service deliberately omits a dependency other similar services have). Do not restate the method/class name in prose form.
