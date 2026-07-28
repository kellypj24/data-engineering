# CI/CD & Infrastructure Hardening Plan

> **Purpose.** A Claude Code–executable backlog of CI/CD and infrastructure
> improvements for this repo.
>
> **How to use this (Claude Code).** Pick the highest-priority unchecked task
> from the index below, implement it, and verify against its **Acceptance**
> criteria. Each task is self-contained — everything needed to do the work is in
> this file. Tick its box in the index and note the PR when done. Do **not**
> batch unrelated tasks into one PR — one task, one reviewable change.
>
> **Scope note.** This repo is a *composable reference toolkit*, not a
> production data platform. Governance should be proportional to that purpose.
> A closing section records patterns that are common in production data
> platforms but are deliberately **not** recommended here, so they don't get
> adopted reflexively.

---

## Task index

Tick these as they land. (Checkboxes only render as checkboxes in a list, which
is why the status lives here rather than on the section headings.)

**P1 — correctness**

- [x] 1. [Fix the `just` module recipes (local task runner is broken)](#1-fix-the-just-module-recipes-local-task-runner-is-broken)
- [x] 2. [Give the dbt tool a `pyproject.toml` + dependabot entry](#2-give-the-dbt-tool-a-pyprojecttoml--dependabot-entry)
- [x] 3. [Lint and test dbt in CI](#3-lint-and-test-dbt-in-ci)
- [ ] 4. [Add a fan-in `ci-success` required check](#4-add-a-fan-in-ci-success-required-check)
- [ ] 5. [Add a repo-wide pre-commit (or lefthook) layer so local == CI](#5-add-a-repo-wide-pre-commit-or-lefthook-layer-so-local--ci)

**P2 — security & supply chain**

- [ ] 6. [Add a scheduled dependency/security audit](#6-add-a-scheduled-dependencysecurity-audit)
- [ ] 7. [Add secret scanning](#7-add-secret-scanning)

**P3 — Claude skills & agent tooling**

- [x] 8. [Bootstrap a `.claude/skills/` library for the toolkit's core workflows](#8-bootstrap-a-claudeskills-library-for-the-toolkits-core-workflows)
- [ ] 9. [Add a `block-no-verify` agent hook](#9-add-a-block-no-verify-agent-hook)

**P4 — AI-assisted CI quality**

- [ ] 10. [Add Claude Code Review on every PR](#10-add-claude-code-review-on-every-pr)
- [ ] 11. [Add scheduled Claude codebase reviews](#11-add-scheduled-claude-codebase-reviews)

**P5 — polish & consistency**

- [ ] 12. [Add a PR template](#12-add-a-pr-template)
- [ ] 13. [Add a semantic PR title check](#13-add-a-semantic-pr-title-check)
- [ ] 14. [Broaden Terraform validation beyond Airbyte](#14-broaden-terraform-validation-beyond-airbyte)
- [ ] 15. [Consider ARM runners for cost/speed](#15-consider-arm-runners-for-costspeed)
- [ ] 16. [Make the shipped dbt tests executable](#16-make-the-shipped-dbt-tests-executable)
- [ ] 17. [Add stack-usage skills for downstream projects](#17-add-stack-usage-skills-for-downstream-projects)

---

## Baseline — what exists today

Grounded in the current `.github/` and tool configs (verified 2026-07-27):

- **`ci.yml`** — `dorny/paths-filter` change detection → per-tool `pytest` +
  a `ruff` lint matrix. Includes cross-paradigm impact detection (dbt/airbyte
  changes trigger the orchestrator tests that wrap them). Every job sets its own
  `working-directory:` and calls `uv` directly — CI never goes through `just`.
- **`terraform-validate.yml`** — `fmt`/`validate`/`test`, scoped to
  `extract_load/airbyte/terraform/**` only.
- **`dependabot.yml`** — 8 entries, weekly: 6 `uv` (dagster, airflow, prefect,
  temporal, dlt, dbt), 1 terraform, 1 github-actions. The Python tools use the
  `uv` ecosystem rather than `pip` so that `uv.lock` is updated alongside
  `pyproject.toml`; the `lockfiles` job in `ci.yml` enforces that invariant.
- **`CODEOWNERS`** — a single wildcard rule: `* @kellypj24`.
- **Task runner** — `just` + `uv`; each tool has its own `mod.just`. All seven
  modules were broken until #63 — see task #1.
- **dbt tool** — has a `pyproject.toml` and `uv.lock` like every other tool, and
  runs credential-free against duckdb by default. `lint-dbt` (sqlfluff +
  yamllint) and `test-dbt` (`dbt parse` + `dbt compile`) run in CI, gated on the
  `dbt` paths-filter. The `dbt-checkpoint` hooks in `.pre-commit-config.yaml`
  are **not** in CI — that tool is pre-commit-only and is picked up by task #5.
- **`.claude/`** — ships `skills/` (`add-tool`, `add-stack`, `verify-tool`) plus
  the skill-writing guide in `skills/CLAUDE.md`. `settings.local.json` is
  gitignored as per-developer. There is no `.cursor/`.

### The gaps, at a glance

| Capability | Today | Task |
|---|---|---|
| Working local task runner | ✅ fixed in #63 | — |
| dbt dependency declaration | ✅ done | — |
| dbt lint in CI | ✅ `lint-dbt` (sqlfluff + yamllint) | — |
| dbt parse/compile in CI | ✅ `test-dbt` | — |
| dbt data tests + unit tests runnable | ❌ no source fixtures | #16 |
| Fan-in required check | ❌ (branch protection can't be meaningful) | #4 |
| Root pre-commit / hooks | ❌ (dbt tool only) | #5 |
| Lockfiles kept in sync with `pyproject.toml` | ✅ `lockfiles` job + `uv` dependabot ecosystem | — |
| Dependency/CVE scan | ❌ | #6 |
| Secret scanning | ❌ | #7 |
| Claude scaffolding skills (`.claude/skills/`) | ✅ add-tool, add-stack, verify-tool | — |
| Stack-usage skills (for downstream projects) | ❌ | #17 |
| Agent-behavior hooks (`.claude/hooks/`) | ❌ | #9 |
| AI-assisted PR review | ❌ | #10 |
| AI scheduled codebase review | ❌ | #11 |
| PR template | ❌ | #12 |
| Semantic PR title check | ❌ | #13 |
| Terraform validation coverage | ⚠️ airbyte only | #14 |
| CODEOWNERS granularity | wildcard (correct for now) | — |

---

## P1 — Correctness gaps (do these first)

### 1. Fix the `just` module recipes (local task runner is broken)

> Landed in PR #63. Kept here with its original framing because tasks #3 and #5
> reference it, and the correction below is worth recording.

**What.** Every recipe in all seven `mod.just` files —
`orchestration/{dagster,airflow,prefect,temporal}`, `extract_load/{dlt,airbyte}`,
and `transformation/dbt` — is prefixed with
`cd {{justfile_directory()}} &&`. Inside a `just` *module*, `justfile_directory()`
resolves to the **root** justfile's directory, not the module's — so every recipe
cd's to the repo root and then runs a command against paths that only exist in
the tool directory:

```console
$ just dagster::test
cd /path/to/data-engineering && uv run pytest tests/ -v
error: Failed to spawn: `pytest`

$ just dbt::lint
cd /path/to/data-engineering && uv run sqlfluff lint models/ macros/
error: Failed to spawn: `sqlfluff`
```

The aggregate root recipes (`just test`, `just lint`, `just fmt`) fail for the
same reason.

> **Correction.** An earlier draft of this task claimed `just airbyte::*` was
> the sole exception because it used `source_directory()`. That is true only on
> the unmerged `feature/airbyte-terraform-setup` branch. On `main`,
> `extract_load/airbyte/mod.just` has the same bug in the form
> `cd {{justfile_directory()}}/terraform`. All seven modules were affected.

**Why.** This is the largest correctness gap in the repo and it invalidates
several tasks below. CI is green only because it bypasses `just` entirely with
explicit `working-directory:` keys, so the breakage is invisible from PR status.
Anyone who clones this toolkit and runs the documented `just dagster::test` from
`CLAUDE.md` hits it immediately. Task #3 wants to lint dbt the same way the
justfile does, and task #5's entire premise is "local == CI" — neither is
meaningful while the local path doesn't run.

**How.** Delete the `cd {{justfile_directory()}} && ` prefix from every recipe.
`just` **already** sets a module recipe's working directory to that module's
directory, so no replacement is needed:

```just
# before
test:
    cd {{justfile_directory()}} && uv run pytest tests/ -v

# after
test:
    uv run pytest tests/ -v
```

Verified against `just 1.46.0` — a probe recipe in `transformation/dbt/mod.just`
reports `cwd=<repo>/transformation/dbt` while `justfile_directory()` reports
`<repo>`. Where a recipe needs a *sub*directory, a plain relative path is enough
(`cd terraform && …` in airbyte's case), since cwd is already the module
directory. All seven files change.

Keep the leading four-space indentation when removing the prefix — deleting it
along with the `cd` un-indents the recipe body, and `just` then fails to parse
the file at all (`error: Unknown start of token '-'`).

**Acceptance.** `just dagster::test`, `just airflow::test`, `just prefect::test`,
`just temporal::test`, and `just dlt::test` each run that tool's suite from that
tool's directory. `just test` and `just lint` run clean from the repo root.
(`just dbt::lint` still needs task #2 to install sqlfluff.)

---

### 2. Give the dbt tool a `pyproject.toml` + dependabot entry

**What.** `transformation/dbt/` is the only tool without a `pyproject.toml`.
Its `mod.just` calls `uv run dbt` and `uv run sqlfluff`, but nothing in the repo
declares those as dependencies, and `uv` finds no project to resolve against.
Add `transformation/dbt/pyproject.toml` declaring `dbt-core`, `dbt-snowflake`,
`sqlfluff`, and `sqlfluff-templater-dbt`, then add the matching
`/transformation/dbt` pip entry to `.github/dependabot.yml`.

While there, single-source the version. `transformation/dbt/.pre-commit-config.yaml`
hardcodes `dbt-core>=1.8.0` / `dbt-snowflake>=1.7.0` / `sqlfluff-templater-dbt>=3.0.0`
in two `additional_dependencies` blocks. Once `pyproject.toml` exists, those are a
second source of truth that dependabot will not keep in sync — derive them at
runtime, or at minimum add a comment in both files pointing at the other. The
rule to hold: **derive the dbt version from `pyproject.toml`, never hardcode it.**

**Why.** This is a hard prerequisite for task #3: CI cannot lint or test dbt
until something declares what to install. It also closes the dependabot hole —
dbt is currently the one tool whose pins never get bumped.

> **What this actually uncovered — DONE.** Because nothing declared dbt's
> dependencies, nobody had ever been able to run this project, and three
> separate blockers had accumulated behind that. All are fixed:
>
> 1. **`profiles.yml` was structurally invalid.** The profile `data_warehouse`
>    held four adapter blocks each with their own `target`/`outputs`, but dbt
>    expects `target` and `outputs` directly under the profile name. `dbt parse`
>    failed with *"outputs not specified in profile 'data_warehouse'"*. Now one
>    profile whose `target` is `{{ env_var('DBT_TARGET', 'duckdb') }}`, with all
>    four warehouses as `outputs` — which is what the file's own comments always
>    described.
> 2. **`packages.yml` named packages that don't exist.** `dbt-labs/dbt_audit_helper`
>    is not in the index (it is `dbt-labs/audit_helper`), `calogica/dbt_expectations`
>    now redirects to `metaplane/`, and `calogica/dbt_date` to `godatadriven/`.
>    dbt_date must match the fork `dbt_expectations` pulls transitively, or dbt
>    errors on a duplicate project name.
> 3. **`.sqlfluff` configured a rule that does not exist.** `convention.count_zero`
>    is silently ignored by sqlfluff (it warns); the real rule is CV04
>    `convention.count_rows`.
>
> The default target is duckdb specifically so the whole toolchain — `dbt parse`,
> `dbt build`, and sqlfluff's dbt templater — runs with **no credentials**, which
> is what task #3 needs in CI.

**How.**
- Mirror the shape of `extract_load/dlt/pyproject.toml` (same `[project]` /
  `[dependency-groups]` layout, Python 3.11+).
- Runtime deps: `dbt-core`, `dbt-snowflake`. Dev group: `sqlfluff`,
  `sqlfluff-templater-dbt`, `yamllint`, `pre-commit`.
- Match the floors already asserted in `.pre-commit-config.yaml` so the two
  agree on day one.
- Add to `.github/dependabot.yml`, matching the existing pip blocks:
  ```yaml
  - package-ecosystem: pip
    directory: /transformation/dbt
    schedule:
      interval: weekly
    open-pull-requests-limit: 5
  ```

**Acceptance.** `just dbt::lint` runs sqlfluff against `models/` and `macros/`
and reports real lint results (requires task #1). `uv sync --dev` succeeds in
`transformation/dbt/`. Dependabot opens dbt dependency PRs.

---

### 3. Lint and test dbt in CI

> **Depends on #1 and #2.** #2 is done, so the lint half is now unblocked:
> `uv run sqlfluff lint models/ macros/` works credential-free against the
> duckdb default target.
>
> **Known blocker for the `test-dbt` half.** `dbt build` currently fails with
> `Catalog Error: Table with name "raw.customers" does not exist` — the example
> models read from `{{ source('raw', ...) }}`, and nothing creates that schema.
> Decide the shape before implementing: either add seeds under `seeds/` that
> materialise a small `raw` fixture, or scope `test-dbt` to `dbt parse` +
> `dbt build --empty` (schema-only, no rows), which validates compilation and
> DDL without needing fixture data. The latter is probably the right fit for a
> toolkit — it demonstrates the CI pattern without pretending to have a dataset.
>
> **Resolved — DONE.** `--empty` turned out **not** to work: it limits rows for
> *models*, but the tests declared on `_sources.yml` still query `raw.orders`
> and `raw.customers`, so `dbt build --empty` fails identically. The shipped
> **unit test cannot run either** — dbt introspects the source relation to infer
> column types and errors with *"Not able to get columns for unit test 'orders'
> from relation `dev.raw.orders` because the relation doesn't exist."*
>
> `test-dbt` therefore runs **`dbt parse` + `dbt compile`**, which validates
> schema YAML, `ref()`/`source()` resolution, macro syntax, and full Jinja
> rendering of every model — all credential-free. See task #16 for making the
> data tests and unit tests executable, which needs source fixtures and is a
> change to the example project's design, not to CI.

**What.** The dbt tool is a first-class `Ready` tool in this repo, yet `ci.yml`
never lints or tests it. dbt changes today only trigger the *orchestrator*
tests via cross-paradigm impact — a broken model or malformed YAML lands
unchecked. Add a `lint-dbt` job (sqlfluff + yamllint + dbt-checkpoint) and a
`test-dbt` job gated on the existing `dbt` paths-filter output.

**Why.** The repo advertises dbt as production-ready but has zero automated
verification of it. The lint config already exists in
`transformation/dbt/.pre-commit-config.yaml` — CI just needs to run it.

**How.**
- Add a `lint-dbt` job (or a `dbt` row in the existing `lint` matrix) running
  `uv run sqlfluff lint models/ macros/` with `working-directory:
  transformation/dbt`, matching how the other lint matrix rows work.
- Add a `test-dbt` job that runs the pre-commit hooks (`sqlfluff-lint`,
  `yamllint`, `dbt-checkpoint`) — or `uv run dbt build` against a DuckDB/
  ephemeral target if one is ever wired, mirroring how the other tools use
  mocked services. **No live Snowflake connection.**
- Gate both on `needs.detect-changes.outputs.dbt == 'true'`. The `dbt` filter
  already exists in `detect-changes`; note it is currently consumed only by
  `resolve-impacts` for cross-paradigm fan-out.
- Add the new jobs to the `ci-success` fan-in from task #4 if that has landed.

If warehouse-backed testing is ever added here, the pattern worth adopting is
building only *changed* models via `dbt build --defer` against a stored base
manifest — but that requires a live warehouse and is out of scope today.

**Acceptance.** A PR that breaks a `.sql` model's lint or removes a required
model description/test fails CI. A clean dbt PR passes. No warehouse connection
required.

---

### 4. Add a fan-in `ci-success` required check

**What.** With change-detection CI, individual jobs *skip* on unrelated PRs.
That makes branch protection awkward — you can't require a job that legitimately
skips. Add one `ci-success` job that `needs` every conditional job, runs
`if: always()`, and fails only if any dependency `failure`d or was `cancelled`
(skipped is OK). Make **that** the single required status check.

**Why.** Without it, branch protection either blocks on jobs that skip
(false negatives) or requires nothing (no gate). The fan-in job is the clean
solution and enables meaningful branch protection.

**How.** Append to `.github/workflows/ci.yml`, keeping `needs` in sync with the
full job list as new jobs are added:

```yaml
  ci-success:
    name: CI Success
    runs-on: ubuntu-latest
    if: always()
    needs:
      - detect-changes
      - resolve-impacts
      - test-dagster
      - test-airflow
      - test-prefect
      - test-temporal
      - test-dlt
      - lint
      - lint-dbt
      - test-dbt
      - lockfiles
    steps:
      - name: Fail if any dependency failed or was cancelled
        if: >-
          contains(needs.*.result, 'failure') ||
          contains(needs.*.result, 'cancelled')
        run: |
          echo "One or more required jobs did not succeed:"
          echo '${{ toJSON(needs) }}'
          exit 1
      - name: Success
        run: echo "All required jobs passed or were skipped."
```

Then set `ci-success` as the only required status check in the repo's
branch-protection settings (Settings → Branches → `main`).

**Acceptance.** `ci-success` reports on every PR; green when all relevant jobs
pass or skip, red when any fail. It is the only required check.

---

### 5. Add a repo-wide pre-commit (or lefthook) layer so local == CI

> **Depends on #1** — a hook layer that shells out to a broken `just` is worse
> than none.

**What.** There's no root-level hook config — only the dbt tool has one. Add a
root `.pre-commit-config.yaml` (or `lefthook.yml`) covering ruff (all tools),
sqlfluff/yamllint (dbt), and file hygiene (trailing whitespace, EOF), and have
CI run the *same* hooks. That way a green local commit is a green CI run.

**Why.** The most reliable way to keep CI fast and contributors unblocked is to
make the local hook set and the CI lint set identical. Right now they can't
diverge, because the local set doesn't exist.

**How.** Either:
- **pre-commit** (simpler): root `.pre-commit-config.yaml`; CI step runs
  `pre-commit run --all-files`.
- **lefthook** (integrates with `just`): tagged hooks; CI runs the relevant tag,
  then `git diff --exit-code` so a hook that auto-fixed a file fails the build
  instead of silently passing.

Prefer pre-commit unless the per-tool `just` integration is worth the extra
moving part — the dbt tool already uses pre-commit, so that's one less tool.
Whichever is chosen, carry over the `git diff --exit-code` guard: a linter that
rewrites a file in CI must fail, not quietly pass.

**Acceptance.** Running the hooks locally produces the same pass/fail as CI.
A PR that would fail lint fails identically whether caught locally or in CI.

---

## P2 — Security & supply chain

### 6. Add a scheduled dependency/security audit

**What.** No CVE scanning today. Add a daily `security-audit.yml` that runs
`pip-audit` across every tool's dependencies and upserts a single tracker issue
(created/updated when vulns exist, auto-closed when clean).

**Why.** This repo pins seven dependency ecosystems. Dependabot bumps versions
but doesn't *alert on known CVEs* in what's currently pinned.

**How.** Loop `pip-audit` over each tool directory (reuse the tool list from
`ci.yml`'s lint matrix, plus dbt once task #2 lands). Run on `ubuntu-latest`,
triggered by `schedule` + `workflow_dispatch`. For the tracker issue, use
`actions/github-script` to search for an open issue with a fixed marker in the
title, then create/update/close it — one issue, not one per run. Report-only:
never gate PRs on it.

**Acceptance.** A known-vulnerable pin surfaces as a labeled `security` issue;
the issue auto-closes when the vuln is resolved. No effect on PR status.

---

### 7. Add secret scanning

**What.** Add a `gitleaks` (or `trufflehog`) job on PRs to catch committed
credentials.

**Why.** Cheap, high-value insurance for a public repo people clone as a
starter — and this one has Terraform, `.env`-driven tools, and connector configs
where a key could slip in. Note that `.gitignore` already excludes `.env`,
`*.pem`, `credentials*.json`, and `terraform.tfvars` — this is the backstop for
when someone `-f`'s past it or adds a secret in a file that doesn't match those
patterns.

**How.** Add `gitleaks/gitleaks-action` on `pull_request`; fail on findings.
Add to the `ci-success` `needs` list if task #4 has landed. Keep it server-side
and unbypassable rather than a local hook — the point is that it can't be
skipped.

**Acceptance.** A PR that adds a fake AWS key or Snowflake password fails.

---

## P3 — Claude skills & agent tooling (the highest-leverage gap)

> **This is the biggest missed opportunity.** This repo's *product* is reusable
> patterns — yet it has **nothing committed under `.claude/` and no `.cursor/`
> directory at all**. A composable toolkit that ships no agent-scaffolding
> skills is leaving its highest-value surface empty: skills are how a Claude
> Code session (or a downstream team that clones a stack) actually *produces*
> correct code against these patterns instead of re-deriving them each time.
>
> **The architecture worth adopting:**
>
> - **Technical / scaffolding skills** — `.claude/skills/<name>/SKILL.md`,
>   repo-local, auto-discovered, *model-invoked* via a strong `description:`.
>   They teach "how we build X here" — one skill per recurring scaffold
>   (add a tool, add a stack, add a dbt model, add a Dagster asset).
> - **Agent-behavior hooks** — a `PreToolUse` hook that hard-denies
>   `git commit/push --no-verify`. Enforcement the model can't rationalize
>   around. See task #9.
>
> **The cardinal rule:** *skills are procedure, `CLAUDE.md` is convention.*
> Skills reference the conventions in `CLAUDE.md`; they never restate them —
> three copies of a style guide drift, and the linters outlive them all.

### 8. Bootstrap a `.claude/skills/` library for the toolkit's core workflows

**What.** Stand up `.claude/skills/` with a small set of scaffolding skills that
mirror the operations this repo already documents in prose (README "Adding a New
Tool", per-tool READMEs). Start with the two highest-frequency ones:

- **`add-tool`** — creates a new tool and wires it in. Note that
  `<role>/_template/` is a **specification README** describing what a tool of
  that role must provide — it is *not* a code skeleton, and there is nothing to
  copy. The skill reads it as a requirements checklist and generates the files,
  modelled on `extract_load/dlt/` (the smallest complete tool). The steps: add
  `pyproject.toml` / `uv.lock` / `README.md` / `CLAUDE.md` / `mod.just`, add
  tests, update the root README table + justfile module imports, add the CI
  paths-filter entry. A phased skill fits well — scaffold → wire into
  `justfile`/CI → verify.
- **`add-stack`** — composes an existing set of tools into a new `stacks/<name>/`
  with its README, env config, and a working example pipeline.

Add per-role scaffolds as they prove useful (`dagster-asset`, `dlt-pipeline`,
`dbt-model`).

**Why.** These are exactly the tasks a person (or an agent) does when adopting
the toolkit, and they're error-prone precisely because they touch ~6 files
across CI, the task runner, and docs. A skill makes the "did you update the
README table / the paths-filter / the justfile import?" checklist executable and
self-verifying — and it *demonstrates* the pattern to downstream teams.

**How.**
- One directory per skill, kebab-case, containing `SKILL.md` with `name` /
  `description` / `argument-hint` frontmatter. Make `description` a strong
  "Use when…" trigger — that string is the only thing the model matches on.
- Reference this repo's `CLAUDE.md` for conventions (uv/just, per-tool
  independence, mocked-services testing) — **do not restate them**.
- End each skill with a **Common mistakes** section and a verify step
  (`just --list`, `just <tool>::test`, the CI paths-filter entry).
- Add a `.claude/skills/CLAUDE.md` stating the procedure-vs-convention rule and
  the file-layout conventions, so the library stays disciplined as it grows.
- If a skill grows past a few phases, split its prompts into a `prompts/`
  subdirectory and have it write a resumable context file, so a long scaffold
  can be picked up mid-run.
- Note that `.claude/settings.local.json` is developer-local; make sure whatever
  ignore rules land don't accidentally exclude the committed skills.

**Acceptance.** In a Claude Code session opened here, "add a new dlt pipeline
tool" (or similar) triggers the skill by description, generates the tool against
its role's requirements, wires it into the `justfile` + CI paths-filter +
dependabot + README table, and the result passes `just --list` and its own test
job with no manual cleanup.

### 9. Add a `block-no-verify` agent hook

> **Best paired with #5** — the hook protects a hook layer, so it's most useful
> once one exists. Harmless to land earlier.

**What.** Add `.claude/hooks/block-no-verify.py` plus a `.claude/settings.json`
`PreToolUse` registration so an agent can't bypass commit hooks with
`--no-verify` / `LEFTHOOK=0` / `PRE_COMMIT_ALLOW_NO_CONFIG=1`.

**Why.** Skills and hooks are only as good as their enforcement. This is a tiny
guardrail that keeps AI-authored commits honest — and, again, models the pattern
for downstream clones.

**How.** Create `.claude/hooks/block-no-verify.py`:

```python
#!/usr/bin/env python3
"""Deny git commit/push --no-verify and hook-disabling env vars."""
import json
import re
import sys

PATTERNS = [
    r"\bgit\b.*\b(commit|push)\b.*(--no-verify|(?<!\w)-n(?!\w))",
    r"\bLEFTHOOK=(0|false)\b",
    r"\bPRE_COMMIT_ALLOW_NO_CONFIG=1\b",
    r"\bSKIP=[\w,]+\s+.*\bgit\b.*\bcommit\b",
]

payload = json.load(sys.stdin)
if payload.get("tool_name") != "Bash":
    sys.exit(0)

command = payload.get("tool_input", {}).get("command", "")
if any(re.search(p, command) for p in PATTERNS):
    print(json.dumps({
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "deny",
            "permissionDecisionReason": (
                "Commit hooks may not be bypassed. Fix the failing check "
                "instead of skipping it; if the hook itself is wrong, fix "
                "the hook."
            ),
        }
    }))
    sys.exit(0)

sys.exit(0)
```

Register it in `.claude/settings.json` (committed, not `settings.local.json`):

```json
{
  "hooks": {
    "PreToolUse": [
      {
        "matcher": "Bash",
        "hooks": [
          {
            "type": "command",
            "command": "$CLAUDE_PROJECT_DIR/.claude/hooks/block-no-verify.py"
          }
        ]
      }
    ]
  }
}
```

`chmod +x` the script. Verify the hook payload shape against the current Claude
Code hooks docs before committing — the schema is versioned, and the snippet
above is written from the documented `PreToolUse` contract rather than copied
from a running example.

**Acceptance.** An agent attempting `git commit --no-verify` in this repo is
denied with a clear reason; a normal `git commit` is unaffected.

---

## P4 — AI-assisted CI quality

> This repo is a *reference toolkit* others clone. Dogfooding AI review here is
> unusually high-signal: the patterns you codify become the patterns downstream
> projects inherit.

### 10. Add Claude Code Review on every PR

**What.** A `claude-code-review.yml` that runs on `opened`/`ready_for_review`,
reads the diff, and posts a single concise review comment focused on bugs,
CLAUDE.md-convention violations, missing tests, and perf concerns.

**Why.** Automated, consistent first-pass review — especially valuable on a
solo-owned repo (`* @kellypj24`) where there's no second human reviewer by
default. It enforces this repo's `CLAUDE.md` conventions on every change.

**How.** Use `anthropics/claude-code-action`. Requires either an
`ANTHROPIC_API_KEY` repo secret or an AWS OIDC role for Bedrock — the API key is
the lower-friction choice here. Use a sticky comment so re-runs update one
comment instead of stacking. Restrict `--allowed-tools` to
`gh pr comment`/`diff`/`view`. Tune the prompt to reference *this* repo's
conventions: composable-tool boundaries, `uv`/`just` usage, per-tool
independence, mocked services in tests. Skip bot-authored PRs (dependabot).

**Acceptance.** Every non-bot PR gets exactly one Claude review comment; "no
significant issues" is a valid one-line result. No multi-comment spam.

### 11. Add scheduled Claude codebase reviews

**What.** A reusable `claude-codebase-review.yml` invoked by thin wrapper
workflows on a schedule, each pointing at a prompt file in `.github/prompts/`.
Findings are filed as labeled issues (`claude-review`, `claude-review:<type>`).

**Why.** Catches drift the per-PR review can't see: architectural erosion, stale
docs, coverage gaps across the whole toolkit. For a repo whose *product* is good
patterns, this is a durable quality flywheel.

**How.** One reusable workflow plus three wrappers, with prompts adapted to this
repo:
- `architecture` — do tools stay independent? do stacks compose cleanly? does
  each tool provide what its role's `_template/README.md` specifies?
- `documentation` — are tool READMEs, the root README table, and `CLAUDE.md` in
  sync with the code? (This plan's own baseline section is a good example of
  what goes stale.)
- `test-coverage` — which tools/assets lack tests; is the mocked-services rule
  upheld?

Create the labels idempotently on each run.

**Acceptance.** Each scheduled run either files/updates a labeled issue or
reports clean.

---

## P5 — Polish & consistency

### 12. Add a PR template

Add `.github/pull_request_template.md` with sections for summary, tool(s)
touched, how tested, and a checklist (tests pass, docs updated, README table
updated if a tool/stack was added). Keep it lean — this isn't a regulated
platform.

### 13. Add a semantic PR title check

Advisory-to-enforced Conventional-Commit title check so history stays clean
(and future release automation is possible). Use
`amannn/action-semantic-pull-request`. Start advisory; flip to enforced once the
habit sticks.

### 14. Broaden Terraform validation beyond Airbyte

`infrastructure/terraform/{snowflake,aws,modules}` exist as placeholders but
aren't validated — `terraform-validate.yml` is scoped to
`extract_load/airbyte/terraform/**` only. As they fill in, widen the `paths`
filter and working directories to cover `infrastructure/terraform/**`, and add
`tflint init` + `tflint` alongside the existing `fmt`/`validate`/`test`.

### 15. Consider ARM runners for cost/speed

`ubuntu-24.04-arm` runners are cheaper and often faster. Low-effort swap for the
pure-Python/lint jobs here. Verify each tool's deps have arm64 wheels first.

### 16. Make the shipped dbt tests executable

The dbt project ships 7 data tests and 1 unit test that **cannot run**, because
the sources they depend on (`raw.orders`, `raw.customers`) do not exist anywhere.
`dbt build` fails with `Catalog Error: ... schema "raw" does not exist`, and the
unit test fails earlier still — dbt introspects the source relation to infer
column types before it can substitute the mock rows.

For a toolkit this matters more than it would in a working repo: the unit test is
there to *demonstrate* dbt's `>=1.8` unit-testing feature, which `dbt_project.yml`
calls out explicitly in `require-dbt-version`. A demonstration that errors on
first run teaches the wrong thing.

Standing the sources up needs a design decision, which is why it is not folded
into #3: seeds land in the schema chosen by `macros/overrides/generate_schema_name.sql`,
which prefixes with the target name outside prod — so a seed intended as
`raw.orders` materialises as `main_raw.orders` against the duckdb default and no
longer matches `source('raw', 'orders')`. Options: give the seeds an explicit
schema config that bypasses the override, relax the override for seeds, or
create the raw relations with a small `on-run-start` hook.

Once sources resolve, extend `test-dbt` in `ci.yml` from `dbt parse` + `dbt
compile` to a full `dbt build`, which then covers both the data tests and the
unit test.

### 17. Add stack-usage skills for downstream projects

Task #8 shipped skills for working **on** the toolkit. This is the other half:
skills for working **with** a stack after someone has chosen one and copied it
into a real project. That is the toolkit's actual product — the skills are how a
downstream team writes correct code against these patterns instead of
re-deriving them.

The critical constraint, stated in `.claude/skills/CLAUDE.md`: these must **not**
assume this repo's directory layout. They run in the downstream project, where
the tool sits at some other path. Take the tool root as an argument or infer it
from `dbt_project.yml` / `pyproject.toml`. A skill that hardcodes
`transformation/dbt/` is useless the moment it is copied.

Candidates, in rough order of how often they are needed:

- **`dbt-model`** — scaffold a staging / intermediate / mart model plus its
  `.yml`, honouring this project's macro conventions (`audit_columns`,
  `clean_string`, `limit_data_in_dev`) and the layer materialisations in
  `dbt_project.yml`. Must emit a description and at least one test per model, or
  the `dbt-checkpoint` hooks reject it.
- **`dbt-source`** — add a source table to a `_sources.yml` with tests, and
  generate the matching staging model.
- **`dagster-asset`** — scaffold an asset, wire it into `Definitions`, and add
  the resource keys it needs.
- **`dlt-pipeline`** — scaffold a dlt source and pipeline with a mocked test.
- **`run-stack`** — bring a chosen stack up locally and prove it end to end:
  EL loads, dbt builds, the orchestrator sees the assets.

Ship them a couple at a time, each with the **Verify** and **Common mistakes**
sections that `.claude/skills/CLAUDE.md` requires. Ground every one in a real
file in this repo rather than an invented example.

---

## Context-dependent — deliberately NOT recommended (yet)

Common patterns in mature production data platforms that **don't** fit a
reference toolkit today. Documented so we don't adopt them reflexively:

- **A `dev → staging → main` promotion path + merge-commit enforcement.**
  Justified when a live production warehouse is downstream of `main`; overkill
  for a clone-to-start toolkit. Revisit only if this repo ever deploys something.
- **PHI/PII scanning.** No regulated data here; #7 (secret scanning) is the
  right-sized substitute.
- **Domain- or path-partitioned CODEOWNERS.** The `* @kellypj24` wildcard is
  correct for a solo-owned repo. Partition only when there are multiple owners
  to route to.
- **Release automation** (e.g. `release-please`). Adopt only if the toolkit
  starts shipping versioned artifacts.
- **A global-skill directory synced to `~/.claude/skills/`.** Personal workflow
  skills (commit, PR, standup, ticket-filing) belong in a dotfiles repo, not in
  a toolkit others clone. Ship repo-local `.claude/skills/` only (#8).

---

## Unrelated debt spotted while writing this

Not CI/CD, but noted so it isn't lost:

- `.gitmodules` declares a `datascience-dagster` submodule with **no
  corresponding gitlink entry in the index** and no directory in the tree.
  `git submodule status` returns nothing. Either restore the submodule or
  delete `.gitmodules`.

---

## Suggested sequencing

1. **#1 (fix `just`)** — everything local is broken until this lands, and #3/#5
   depend on it. Smallest diff in the plan; do it first.
2. **#2 (dbt `pyproject.toml`)** → **#3 (dbt in CI)** — closes the largest
   verification hole, in that order.
3. **#4 (fan-in check)** — then turn on meaningful branch protection.
4. **#5 (local == CI hooks)** — now that both halves actually run.
5. **#8 (`.claude/skills/` library)** — highest leverage for a patterns toolkit;
   the scaffolding skills make every subsequent "add a tool/stack" correct by
   construction, and demonstrate the pattern to downstream clones.
6. **#10 (Claude PR review)** — biggest CI quality-per-effort win, especially solo.
7. **#6 / #7 (security)** and **#9 (block-no-verify hook)** — cheap guardrails.
8. **#11 (scheduled AI reviews)** and **#12–#15 (polish)** — as capacity allows.
