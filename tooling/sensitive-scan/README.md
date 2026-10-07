# sensitive-scan

A high-precision guard that stops secrets, PII, and identity literals from being
committed. For projects whose data is regulated, where secrets scanning alone
is not enough. One stdlib-only script runs as a pre-commit hook and as a CI
step.

This toolkit's own CI uses gitleaks for secrets; `sensitive-scan` is a tool for
**downstream** projects.

## Packs

| Pack | Finds | Stays quiet on |
|------|-------|----------------|
| `secrets` | AWS keys, private keys, GitHub and Slack tokens. **Uses gitleaks when installed**; these patterns are the fallback | non-key strings with the same prefix |
| `pii` | SSN-shaped values, phone numbers, email addresses, a personal name with a date of birth on the same line | reserved SSN areas, 555-01xx fictional numbers, `example.com`/`.test`/`.invalid`, git remotes (`git@host:path`), `a@b.com`-style placeholders, a date not directly after a DOB key |
| `identity-literals` | a literal UUID compared against a configured identity column; tab/pipe dump rows with an id and a date | UUIDs compared against other columns; header rows |

Findings print as `path:line: [pack/rule] ma*****ed`: the value is masked so the
scanner never copies it into a terminal or CI log. Exit codes: 0 clean, 1
findings, 2 bad config.

## The one exemption

Add `sensitive-scan:ignore` to the line itself, for synthetic fixtures:

```csv
1001,Maria Lopez,dob 1984-03-17  # sensitive-scan:ignore -- synthetic
```

There is **no** directory or path skip list. The config loader rejects keys like
`exclude`, `skip_paths`, or `ignore_dirs`, because a directory exemption is how
real data ends up in a "tests" folder.

## Configure

Copy `sensitive-scan.example.toml` to `.sensitive-scan.toml` at your repo root.
Without one, every pack is on.

## Adopt

Copy this directory into your project (e.g. `tools/sensitive-scan/`).

**pre-commit** (no venv needed: the script is stdlib-only):

```yaml
- repo: local
  hooks:
    - id: sensitive-scan
      name: sensitive-scan
      entry: python3 tools/sensitive-scan/src/sensitive_scan/scan.py
      language: system
      types: [text]
```

**GitHub Actions**: make this a required check, so a commit made with
`--no-verify` still cannot merge:

```yaml
sensitive-scan:
  runs-on: ubuntu-latest
  steps:
    - uses: actions/checkout@v4
    - uses: actions/setup-python@v5
      with: { python-version: "3.12" }
    - run: python3 tools/sensitive-scan/src/sensitive_scan/scan.py --all
```

If sensitive data has already reached git history, follow
[RUNBOOK.md](RUNBOOK.md).

## Develop

```bash
just sensitive-scan::test    # fixture tests, no network (gitleaks is mocked)
just sensitive-scan::scan    # scan this repository
```
