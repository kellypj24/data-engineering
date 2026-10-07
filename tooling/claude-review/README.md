# claude-review

An opt-in, advisory Claude review of the commits you are about to push. It runs
on your machine as a `pre-push` hook, before CI. It complements a CI review
(`docs/ci-cd-hardening.md` #10); it does not replace one.

```bash
CLAUDE_REVIEW=1 git push      # review this push
git push                      # no review
```

It prints findings, and **blocks the push only** when the model ends with
`VERDICT: CRITICAL`. Any other reply, including one with no verdict line, is
advice.

## What it guarantees

- **Fail open.** A missing `claude` CLI, an auth error, a timeout, a reply it
  cannot parse, or a bug in the script prints a warning and lets the push
  through.
- **Only scanned, clean files leave the machine.** Each changed file is checked
  with [sensitive-scan](../sensitive-scan/) first. Files that trip it are
  withheld, and the review is told which. If the scanner cannot load, nothing
  is sent.
- **Bounded.** `CLAUDE_REVIEW_MAX_FILES` (20), `CLAUDE_REVIEW_MAX_BYTES`
  (100000), `CLAUDE_REVIEW_TIMEOUT` seconds (180). Anything cut is announced.
- **Correct ranges.** Reads the push range from `PRE_COMMIT_FROM_REF` /
  `PRE_COMMIT_TO_REF`. A new branch is diffed against its merge-base with the
  remote's default branch, deleting a branch reviews nothing, renames review
  the new path, and deleted files send no content.

## Requirements

- Python **3.11+** as `python3` (stdlib only, no venv). macOS's
  `/usr/bin/python3` is 3.9: the review then sends nothing and allows the push.
- The `claude` CLI on `PATH`, logged in. It runs `claude -p`.
- `tooling/sensitive-scan/` alongside it (the gate), or `sensitive-scan`
  installed.

## Adopt

Copy `tooling/claude-review/` and `tooling/sensitive-scan/` into your project,
then add to `.pre-commit-config.yaml`:

```yaml
default_install_hook_types: [pre-commit, pre-push]
repos:
  - repo: local
    hooks:
      - id: claude-review
        name: claude-review (CLAUDE_REVIEW=1)
        entry: python3 tooling/claude-review/src/claude_review/review.py
        language: system
        stages: [pre-push]
        pass_filenames: false
        always_run: true
```

and run `pre-commit install --hook-type pre-push`.

## Develop

```bash
just claude-review::test   # temporary git repos; `claude` is mocked, no network
```
