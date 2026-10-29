# Status

Current state only; history is in git and the PRs. Update this file in the same PR as the
work it describes.

_Updated 2026-10-28._

## Now

- Round 3 of the roadmap (`docs/toolkit-expansion.md`, `docs/ci-cd-hardening.md`) is in
  progress. Done: E35–E55 (P7–P9); CI hardening #9, #12, #13, #19, #21, #22, #23.

## Next

1. `docs/toolkit-expansion.md` round 3 is done; next is choosing items from its *Deferred to a later round* list.
2. CI hardening #14 (broaden Terraform validation), then #24, #15; #10 and #11 wait on
   the `ANTHROPIC_API_KEY` decision (`docs/ci-cd-hardening.md`).
3. Lock refresh: `uv lock --upgrade` per tool to clear the advisories the daily audit
   reports (`python3 .github/scripts/security_audit.py --out x.md`).
4. Airflow/Prefect parity, lower priority than Dagster: port the round-3 Dagster
   patterns that apply (per-deployment schedule times, SQS sensor, warehouse backup)
   so they don't fall far behind. Dagster stays the reference.

## Open decisions

- `DEPS_PR_TOKEN` secret, or allow Actions to create PRs, so the compile gate opens PRs
  instead of issues (#19).
- `ANTHROPIC_API_KEY` secret for Claude PR review (#10) and scheduled reviews (#11); both
  skip cleanly without it.
- Major Dependabot bumps held on red CI: #89 (Airbyte Terraform), #158 (Airflow 3), and
  #291 (duckdb 2.0 in `tooling/release-diff`).

## Blocked on

Nothing.
