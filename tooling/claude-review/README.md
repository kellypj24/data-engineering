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

- **Fail open, and say so.** A missing `claude` CLI, an auth error, a timeout,
  a reply it cannot parse, or a bug in the script lets the push through with a
  warning starting `UNAVAILABLE (NOT A PASS):`, so a skipped review never reads
  as a passing one.
- **Isolated session.** The model gets the prompt and nothing else: no tools,
  no MCP servers (`--strict-mcp-config` with an empty config), safe mode (no
  hooks, settings, or `CLAUDE.md`), and no transcript written to disk. An older
  CLI that rejects one of these flags fails open; it is never retried without
  the flag.
- **Only scanned, clean content leaves the machine.** Both sides of each
  changed file, base and head, are checked with
  [sensitive-scan](../sensitive-scan/) before anything is sent, because a diff
  carries the removed lines too: deleting a secret would otherwise send it.
  Files where either side trips it are withheld, and the review is told which
  side. If the scanner cannot load, nothing is sent.
- **Bounded.** `CLAUDE_REVIEW_MAX_FILES` (20), `CLAUDE_REVIEW_MAX_BYTES`
  (100000), `CLAUDE_REVIEW_TIMEOUT` seconds (180). Anything cut is announced.
- **Correct ranges.** Reads the push range from `PRE_COMMIT_FROM_REF` /
  `PRE_COMMIT_TO_REF`. A new branch is diffed against its merge-base with the
  remote's default branch, deleting a branch reviews nothing, renames review
  the new path, and deleted files send no content.

## Requirements

- The `claude` CLI on `PATH`, logged in, recent enough to accept `--safe-mode`
  and `--no-session-persistence`. `CLAUDE_REVIEW_MODEL` picks the model
  (default: the CLI's).
- sensitive-scan importable by the same Python (the gate). Without it the
  review sends nothing.
- Python **3.11+**. Installed as a tool, uv provides it. Vendored, it runs as
  `python3` (stdlib only, no venv); macOS's `/usr/bin/python3` is 3.9, and the
  review then sends nothing and allows the push.

## Adopt

Two ways in. Both enable the review per repo through pre-commit, and both run it
only on `CLAUDE_REVIEW=1 git push`.

### Install once per machine (many repos)

Install both tools into one uv environment, pinned to a commit of this toolkit:

```bash
REF=<commit sha>
SRC="git+https://github.com/kellypj24/data-engineering@$REF"
uv tool install --force --python ">=3.11" \
  "claude-review @ $SRC#subdirectory=tooling/claude-review" \
  --with "sensitive-scan @ $SRC#subdirectory=tooling/sensitive-scan"

# The gate must import from the tool's environment:
"$(uv tool dir)/claude-review/bin/python" -c "import sensitive_scan.scan"
```

Bump `REF` deliberately, after reading the diff of `tooling/claude-review/`
and `tooling/sensitive-scan/` between the two commits.

In each repo, add to `.pre-commit-config.yaml`:

```yaml
default_install_hook_types: [pre-commit, pre-push]
repos:
  - repo: local
    hooks:
      - id: claude-review
        name: claude-review (CLAUDE_REVIEW=1)
        entry: claude-review
        language: system
        stages: [pre-push]
        pass_filenames: false
        always_run: true
```

and run `pre-commit install --hook-type pre-push`. A `.sensitive-scan.toml` in
the repo picks scanner packs; without one, every pack is on.

### Vendor into one project

Copy `tooling/claude-review/` and `tooling/sensitive-scan/` into your project
(the review finds the scanner as a sibling directory), then use the same
snippet with:

```yaml
        entry: python3 tooling/claude-review/src/claude_review/review.py
```

### Why not a global git hook

`core.hooksPath` replaces every repo's `.git/hooks`, including the ones
pre-commit installs, and the review reads the push range from pre-commit's
`PRE_COMMIT_FROM_REF` / `PRE_COMMIT_TO_REF`, not from a raw hook's stdin.

## Develop

```bash
just claude-review::test   # temporary git repos; `claude` is mocked, no network
```
