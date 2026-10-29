# Status

Current state only; history is in git and the PRs. Update this file in the same PR as the
work it describes.

_Updated 2026-10-29._

## Now

- Round 3 of `docs/toolkit-expansion.md` is done (E35–E55). CI hardening done: #9, #12,
  #13, #19, #21, #22, #23.
- Round 4 (E56–E74, P10–P14) is drafted; none started.

## Next

1. Round 4 in its work order: E56 (cost dates in UTC) and E57 (Airbyte assets from
   `AirbyteWorkspace`) first, then E58–E59, E60–E70, E71–E74.
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
