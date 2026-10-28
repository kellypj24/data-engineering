# Status

Current state only; history is in git and the PRs. Update this file in the same PR as the
work it describes.

_Updated 2026-10-28._

## Now

- Round 3 of the roadmap (`docs/toolkit-expansion.md`, `docs/ci-cd-hardening.md`) is in
  progress. Done: E35–E45; CI hardening #12, #13, #19, #21, #22, #23.

## Next

1. The rest of P8 (E46 per-deployment schedule times next), then P9 (`docs/toolkit-expansion.md`).
2. CI hardening #9 (block `--no-verify` agent hook), then #10, #11, #14, #15, #24
   (`docs/ci-cd-hardening.md`).
3. Lock refresh: `uv lock --upgrade` per tool to clear the advisories the daily audit
   reports (`python3 .github/scripts/security_audit.py --out x.md`).

## Open decisions

- `DEPS_PR_TOKEN` secret, or allow Actions to create PRs, so the compile gate opens PRs
  instead of issues (#19).
- `ANTHROPIC_API_KEY` secret for Claude PR review (#10) and scheduled reviews (#11); both
  skip cleanly without it.
- Major Dependabot bumps held on red CI: #89 (Airbyte Terraform), #158 (Airflow 3), and
  #291 (duckdb 2.0 in `tooling/release-diff`).

## Blocked on

Nothing.
