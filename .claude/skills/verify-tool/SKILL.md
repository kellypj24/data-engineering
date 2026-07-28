---
name: verify-tool
description: Use when checking whether a tool in this toolkit is healthy — "is dagster still working", "verify the dbt tool", "check all the tools", "did I break anything", or before opening a PR that touches a tool. Runs the local gauntlet that CI does not cover.
argument-hint: "[tool-name]   omit to check every tool"
---

# Verify a tool is healthy

CI checks less than you think. It calls `uv` directly with explicit
`working-directory:` keys and **never goes through `just`** — so every `just`
recipe in this repo can be broken while CI is green. That happened: all seven
`mod.just` files ran in the repo root instead of the tool directory, and the
badge stayed green throughout.

This skill runs what CI cannot.

## The gauntlet

For each tool, in order. Stop at the first failure and report it — do not
continue and summarise at the end.

```bash
cd <role>/<tool>

uv sync --dev            # dependencies resolve and install
uv lock --check          # uv.lock matches pyproject.toml -- no drift
<lint command>           # from the tool's mod.just
<test command>           # from the tool's mod.just
```

Then, from the repo root — this is the part CI skips:

```bash
just --list              # all modules parse
just <tool>::test        # recipe runs in the tool's directory
just <tool>::lint
```

And for the whole repo:

```bash
just test
just lint
```

Take the exact lint/test commands from the tool's `mod.just` rather than
assuming — they differ per tool (`ruff check src/ tests/` for dagster,
`ruff check dags/ tests/` for airflow, sqlfluff for dbt).

## Tool-specific notes

- **dbt** — needs `uv run dbt deps` before sqlfluff will run, because the
  sqlfluff dbt templater compiles the project. `DBT_PROFILES_DIR` is exported by
  its `mod.just`; profiles default to duckdb, so nothing needs credentials.

  Its healthy gauntlet is `uv lock --check`, `just dbt::lint`, `dbt parse`, and
  `dbt compile` — the same four CI runs. **`just dbt::test` and `dbt build` fail
  by design**, because the example models read from `source('raw', …)` and
  nothing creates that schema. Known, tracked as task #16 in
  `docs/ci-cd-hardening.md`. Do not report it as a regression, and do not "fix"
  it by pointing the models somewhere else.
- **airbyte** — `just airbyte::validate` and `::test` need `terraform init
  -backend=false` in `extract_load/airbyte/terraform/` first. There is no `init`
  recipe yet, so on a fresh clone these fail until you run it by hand.

## Reporting

State plainly which checks passed and which failed, with the failing output. If
a failure is pre-existing on `main` rather than caused by current changes, say
so and say how you determined it — usually by running the same check on a clean
checkout of `main`.

## Common mistakes

- **Treating green CI as proof the tool works.** CI bypasses `just` entirely.
- **Running `just fmt` as a check.** It rewrites files. Use the lint recipe, or
  `ruff format --check`, when you mean to verify rather than change.
- **Letting `uv sync` dirty the tree.** It can rewrite `uv.lock` when the lock
  has drifted from `pyproject.toml`. If the tree is dirty afterwards, that is a
  finding — commit the regenerated lock or explain it — not something to
  `git checkout --` away silently.
- **Reporting "all passed" after a command that never ran.** `&&` chains stop at
  the first non-zero exit; a lint failure silently skips the tests behind it.
