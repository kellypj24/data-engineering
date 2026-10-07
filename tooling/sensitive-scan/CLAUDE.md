# sensitive-scan

## Role
Tooling: a commit guard for downstream projects with regulated data. See
README.md for packs and adoption, RUNBOOK.md for incidents.

## Key Files

- `src/sensitive_scan/scan.py` — the whole tool, **stdlib only** (it runs as a git hook without a venv). Packs: `secrets` (gitleaks if installed, built-in fallback), `pii`, `identity-literals`
- `tests/test_scan.py` — one (rule, positive, near-miss) row per rule; every row is also tested with the ignore marker
- `sensitive-scan.example.toml`, `.pre-commit-hooks.yaml`, `RUNBOOK.md`

## Patterns

- **Precision over recall.** A new rule needs a near-miss case in `CASES`; when a real file trips falsely, add it as a near miss before tightening the pattern
- **The only exemption is the per-line `sensitive-scan:ignore` marker.** Never add a path/directory skip; `validate_config` rejects one by design
- Findings are masked; never print the raw value
- Fixture values in tests are assembled at runtime (`"AKIA" + "..."`) so the test file does not trip other scanners
- gitleaks is always mocked in tests; its failure falls back to the built-in patterns, never to scanning nothing
