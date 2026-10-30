# Status

Current state only; history is in git and the PRs. Update this file in the same PR as the
work it describes.

_Updated 2026-10-30._

## Now

- Round 3 of `docs/toolkit-expansion.md` is done (E35–E55). CI hardening done: #9, #12,
  #13, #19, #21, #22, #23.
- Round 4 (E56–E74, P10–P14) in progress: E56 done.
- duckdb 2.0 in dbt and dagster together (a file 2.0 creates is unreadable by 1.x);
  file-export and release-diff via Dependabot #289, #291. dlt stays on 1.4.4 (via
  `dlt[duckdb]`) and writes only its own pipeline files.

## Next

1. Round 4 in its work order: E57 (Airbyte assets from `AirbyteWorkspace`), then
   E58–E59, E60–E70, E71–E74.
2. CI hardening #14 (broaden Terraform validation), then #24, #15; #10 and #11 wait on
   the `ANTHROPIC_API_KEY` decision (`docs/ci-cd-hardening.md`).
3. Lock refresh: `uv lock --upgrade` per tool to clear the advisories the daily audit
   reports (`python3 .github/scripts/security_audit.py --out x.md`; 495 rows across 7
   tools on 2026-10-30, certifi cleared). Lock with a current uv: Homebrew's 0.10.3
   rewrites markers that CI's uv does not.
4. Airflow/Prefect parity, lower priority than Dagster: port the round-3 Dagster
   patterns that apply (per-deployment schedule times, SQS sensor, warehouse backup)
   so they don't fall far behind. Dagster stays the reference.

## Open decisions

- `DEPS_PR_TOKEN` secret, or allow Actions to create PRs, so the compile gate opens PRs
  instead of issues (#19).
- `ANTHROPIC_API_KEY` secret for Claude PR review (#10) and scheduled reviews (#11); both
  skip cleanly without it.

## Blocked on

Nothing.
