# claude-review

## Role
Tooling: opt-in pre-push Claude review. See README.md.

## Key Files

- `src/claude_review/review.py` — the whole hook, **stdlib only**. `run()` is the flow; `main()` wraps it so nothing can raise
- `tests/test_review.py` — real temporary git repos; only the `claude` subprocess is faked (`FakeClaude`)

## Patterns

- **Fail open, always.** Every new failure mode needs a `test_fails_open` case; the only non-zero exit is an explicit `VERDICT: CRITICAL`
- **The gate is sensitive-scan, run before sending.** `Payload.sent` must stay a subset of `Payload.scanned`; scanner unavailable means send nothing
- Never read the push range from stdin (pre-commit consumes it); use `PRE_COMMIT_FROM_REF` / `PRE_COMMIT_TO_REF`
