# Toolkit Expansion Plan

> **Purpose.** A Claude Code–executable backlog of *features* to add to the
> toolkit: dbt building blocks, Dagster patterns, warehouse environment
> management, and shippable quality tools. Its sibling,
> [`ci-cd-hardening.md`](./ci-cd-hardening.md), covers CI, supply chain, and
> repo hygiene for this repo itself. If a task is about how *this repo* is
> checked, it belongs there; if it is something a downstream project copies out
> of a tool or stack, it belongs here.
>
> **How to use this (Claude Code).** Pick the highest-priority unchecked task
> from the index, implement it, verify against its **Acceptance** criteria, tick
> its box, and note the PR. One task, one PR.
>
> **Provenance.** Every task here is a pattern that has already run in a
> production dbt + Dagster platform on Snowflake and been generalised. The
> *why* in each task is the failure that pattern was built to prevent. Nothing
> here should name a company, a client, a business domain, or a vendor dataset —
> if a task needs a worked example, invent a neutral one (`orders`, `customers`).
>
> **Warehouse scope.** The toolkit's dbt project defaults to duckdb and ships
> outputs for Snowflake, Postgres, and BigQuery. Tasks in P1/P2 must run on the
> duckdb default. Tasks marked **Snowflake extra** rely on Snowflake-only
> features (zero-copy clone, ownership, `COPY GRANTS`, Cortex); put them under
> `macros/snowflake/` and say so in their header, rather than pretending they are
> portable.

---

## Task index

**P1 — dbt building blocks**

- [ ] E1. [Surrogate-key minting macro](#e1-surrogate-key-minting-macro)
- [ ] E2. [In-place surrogate-key backfill operation](#e2-in-place-surrogate-key-backfill-operation)
- [ ] E3. [Small generic tests and helpers](#e3-small-generic-tests-and-helpers)
- [ ] E4. [Tiered-severity validation framework](#e4-tiered-severity-validation-framework)

**P2 — Dagster patterns**

- [ ] E5. [Definition invariant tests](#e5-definition-invariant-tests)
- [ ] E6. [One notification model, rendered by severity](#e6-one-notification-model-rendered-by-severity)
- [ ] E7. [Run-status sensor chaining](#e7-run-status-sensor-chaining)
- [ ] E8. [Deployment-routed target database with a fail-safe default](#e8-deployment-routed-target-database-with-a-fail-safe-default)

**P3 — Warehouse environment management (Snowflake extra)**

- [ ] E9. [Snowflake RBAC as code](#e9-snowflake-rbac-as-code)
- [ ] E10. [Zero-copy environment refresh](#e10-zero-copy-environment-refresh)
- [ ] E11. [Preservation manifest for non-dbt objects](#e11-preservation-manifest-for-non-dbt-objects)
- [ ] E12. [Ownership drift audit and repair](#e12-ownership-drift-audit-and-repair)
- [ ] E13. [Write-boundary proof and identity probe](#e13-write-boundary-proof-and-identity-probe)

**P4 — Shippable quality tools**

- [ ] E14. [Add a `tooling/` role](#e14-add-a-tooling-role)
- [ ] E15. [Local Claude pre-push review](#e15-local-claude-pre-push-review)
- [ ] E16. [Configurable sensitive-data commit guard](#e16-configurable-sensitive-data-commit-guard)

**P5 — Skills, docs, and optional extras**

- [ ] E17. [`data-profiling` stack skill](#e17-data-profiling-stack-skill)
- [ ] E18. [`dbt-test` stack skill](#e18-dbt-test-stack-skill)
- [ ] E19. [`backfill-runbook` stack skill](#e19-backfill-runbook-stack-skill)
- [ ] E20. [State migrations on promotion](#e20-state-migrations-on-promotion)
- [ ] E21. [Architecture decision records](#e21-architecture-decision-records)
- [ ] E22. [Semantic-view generator from dbt metadata (Snowflake extra)](#e22-semantic-view-generator-from-dbt-metadata-snowflake-extra)

---

## Baseline — what exists today

Verified 2026-09-24 against `main`:

- **dbt** (`transformation/dbt/`) — five macros (`overrides/generate_schema_name`,
  `staging/{audit_columns,clean_strings}`, `utils/{limit_data_in_dev,safe_divide}`),
  one singular test, one example staging model. No surrogate-key, history, or
  validation macros.
- **Dagster** (`orchestration/dagster/src/`) — assets (airbyte, dbt), one
  freshness check, one S3 sensor, one daily schedule, `utils/alerts.py` (a
  failure-only Slack hook), `utils/factories.py`. Tests assert properties of
  individual definitions (e.g. the one schedule is `STOPPED`), not invariants
  over all of them.
- **Infrastructure** — `infrastructure/terraform/{snowflake,aws,modules}` are
  empty placeholders.
- **Skills** — toolkit skills (`add-tool`, `add-stack`, `verify-tool`) and stack
  skills (`dbt-model`, `dbt-source`, `dagster-asset`, `dlt-pipeline`,
  `run-stack`). No profiling, testing, or backfill skill.
- **No `tooling/` role.** Every tool today is EL, orchestration, or
  transformation; there is nowhere for a developer-side tool to live.

---

## P1 — dbt building blocks

### E1. Surrogate-key minting macro

**What.** Add `macros/utils/mint_surrogate_key.sql` (`mint_surrogate_key(fields,
null_as=none)`) and a companion `surrogate_key_version()` that returns the
current hash-policy version. Output is a UUID-shaped string from one MD5
evaluation. Dispatch through `adapter.dispatch` so it runs on duckdb,
Snowflake, Postgres, and BigQuery.

**Why.** `dbt_utils.generate_surrogate_key` and the usual hand-rolled
"coalesce everything to `''` and hash" macros have three silent defects:

1. **NULL and `''` produce the same key.** Two different grains collapse onto
   one key, and the uniqueness test still passes — the collision *reduces* the
   row count rather than duplicating a value.
2. **Delimiter collisions.** `('a|b', 'c')` and `('a', 'b|c')` concatenate to the
   same string.
3. **No version.** A change of hashing policy is invisible in review and
   indistinguishable in the data.

**How.**
- Replace NULL with a reserved token that is not a plausible data value, so NULL
  is distinct from every real string including `''`. State the residual caveat
  (a literal equal to the token is indistinguishable from NULL) in the header.
- **Length-prefix each field** before concatenating — this removes delimiter
  collisions outright rather than making them unlikely.
- Put the version into the hashed payload, and expose it via
  `surrogate_key_version()` so callers can store it in a `key_hash_version`
  column on any table whose rows are not all minted at once.
- `null_as`: when set, NULL in any field is replaced by that literal before
  hashing, so a fact's nullable foreign key resolves to a real "not applicable"
  dimension member. This matters because a NULL foreign key passes a
  `relationships` test silently. Leave it unset for identity keys.
- **Do not add a `hash_version` override.** Reproducing an older key means
  versioning the *encoding*, not just the prefix; a parameter that swaps only
  the prefix silently produces a third thing. Migrate rows forward with E2
  instead.
- Header comment must say: fields are hashed in a **fixed order**, and changing
  the order or encoding re-mints every key — that is a version bump.

**Acceptance.** A singular test `tests/macros/assert_mint_surrogate_key_properties.sql`
built from literal `values` rows (no sources, so it runs on duckdb today) fails
if: NULL and `''` produce the same key; `('a|b','c')` and `('a','b|c')` collide;
the same inputs produce different keys; or output is not UUID-shaped. `dbt build
--select assert_mint_surrogate_key_properties` passes against the duckdb target.

---

### E2. In-place surrogate-key backfill operation

> **Depends on E1.**

**What.** `macros/utils/backfill_surrogate_keys.sql`, invoked with `dbt
run-operation`, that fills or upgrades key columns **in place** on an existing
table.

**Why.** Adding a key column to an incremental model does not populate history:
`on_schema_change='append_new_columns'` adds the column and leaves it NULL
outside the incremental window. A `--full-refresh` fixes that by re-reading
upstream — which is **unsafe** wherever a source has shorter retention than the
table, because the rebuild silently replaces real history with whatever the
source still holds. A surrogate key is a pure function of columns already in
the row, so an `UPDATE` reproduces exactly what the model would have written,
without reading any source.

**How.** Arguments:
- `relation`, and `key_expressions` — `{column: sql_expression}`, written in one
  `UPDATE`.
- `dependent_keys` — keys that hash *from* columns `key_expressions` writes
  (e.g. a composite key over two parent FKs). Applied in a **second** statement:
  SQL evaluates a `SET` list against pre-update values, so folding them into the
  first statement hashes the NULLs being replaced and mints one identical key
  for every row — a corruption that passes a uniqueness test.
- `version_column` — doubles as the idempotency marker. Touch rows where it is
  NULL or older; write it in the **last** statement so it certifies every pass
  completed. Re-running a finished table is a no-op.
- `scope_predicate` — optional narrowing. Required if `version_column` is none.
- `dry_run` — **defaults to true**; logs the statements without executing.

**Acceptance.** Against a duckdb seed table with NULL keys: a dry run changes
nothing and prints both statements; a real run fills every key; a second real
run updates zero rows; the dependent key differs row-to-row.

---

### E3. Small generic tests and helpers

**What.** Two small additions:
- `macros/utils/import.sql` — `import(ref, alias)` emits `alias as (select * from
  ref)`, so models open with a uniform block of import CTEs.
- `tests/generic/timestamp_is_utc.sql` — fails rows whose timestamp column
  carries a non-UTC offset. Dispatch per adapter; on adapters without offset-aware
  types, document it as a no-op rather than silently passing.

**Why.** Mixed-offset timestamps are the classic silent join bug between two
sources that agree on the wall clock but not the zone. `import()` is a
readability convention the `dbt-model` skill can then enforce.

**Acceptance.** Both are documented in the dbt tool's README macro table and
used once in the example project; `dbt parse` and `dbt compile` stay green.

---

### E4. Tiered-severity validation framework

**What.** A small framework for data-quality rules that are richer than dbt
tests: a `validation_rules` seed (rule id, target model, SQL predicate, severity
`info`/`warn`/`error`/`block`, owner), a macro that applies every rule for a
model and emits one row per violation, and an incremental
`validation_results` model that logs outcomes over time.

**Why.** dbt tests are binary and ephemeral: they pass or fail and then the
evidence is gone. Real operations need (a) severities that decide whether a run
stops, warns, or only records, and (b) a history of violations so trends are
visible. Encoding rules as data rather than as one test file each keeps them
reviewable and lets non-engineers own them.

**How.** Keep the engine to three macros: apply-rules, log-result, and
get-config. `block` severity raises; `error` fails the model's `dbt test`; `warn`
and `info` only log. Ship two example rules on the example project. Write the
decision up as an ADR once E21 exists.

**Acceptance.** Adding a rule row to the seed changes behaviour with no code
change; a `block` rule stops `dbt build`; a `warn` rule logs a row and lets the
build pass; `validation_results` retains prior runs.

---

## P2 — Dagster patterns

### E5. Definition invariant tests

**What.** `orchestration/dagster/tests/test_invariants.py` — tests that load the
full `Definitions` object and assert properties over **every** schedule, sensor,
and job, not just the ones that exist today. Make it importable so each stack's
Dagster code can run the same suite against its own `Definitions`.

**Why.** Per-definition tests only protect the definitions someone remembered to
test. Invariants catch the next one added. Each assertion below exists because
its absence caused a production incident somewhere.

**How.** Assert that:
- every schedule has `default_status == STOPPED` (opt-in activation);
- every sensor that **launches** a job is `STOPPED` by default — while safety-net
  sensors that only observe may be `RUNNING`, listed explicitly by name;
- every schedule's `execution_timezone` is UTC (or one declared constant);
- every cron string is valid (parse it, don't regex it);
- no duplicate schedule, sensor, or job names;
- jobs tagged as manual-only (probes, one-off repairs) are **never** targeted by
  any schedule or sensor;
- if there are multiple `@dbt_assets` definitions, their selections **partition**
  the manifest with no overlap and no gaps;
- dbt jobs use `dbt build` rather than `run` then `test` — and **seeds are not
  excluded** from a build selection. Excluding seeds silently drops their data
  tests, and a selection that starts at a seed cannot run them. Test this by
  resolving the selection against the manifest, not by string-matching it. Note
  that in dbt selectors a comma is an *intersection* and a space is a *union*;
  the test should resolve selectors rather than reason about them.

**Acceptance.** Adding a schedule with `DefaultScheduleStatus.RUNNING`, a
duplicate job name, or a dbt job whose selection excludes seeds each make the
suite fail with a message naming the offender.

---

### E6. One notification model, rendered by severity

**What.** Replace `utils/alerts.py`'s failure-only hook with
`utils/notifier.py`: one `Notification` dataclass (job, severity, summary,
fields, links, run URL) and one Slack Block Kit renderer. Severity decides the
layout — `success` renders as a single scannable line; `warning` and `failure`
expand with fields, a detail block, and links. Severity→channel routing lives in
one function.

Optionally add `utils/telemetry.py`: write one event row per run outcome to a
warehouse table, with **every telemetry error swallowed** so observability can
never fail a pipeline, plus a safety-net sensor that emits a missing
STARTED/FAILED event when the framework itself dies before the hook fires.

**Why.** Per-job ad hoc Slack messages drift until nobody reads them. Separating
rendering from telemetry means an Airbyte or file-transfer job reuses the same
alert standard by building a `Notification`, without touching the telemetry
schema. Routing in one place makes "split warnings into their own channel" a
one-line change.

**Acceptance.** Existing hook call sites use the new module; unit tests render
each severity to Block Kit JSON and snapshot them; a raising telemetry sink does
not fail the job under test.

---

### E7. Run-status sensor chaining

**What.** A worked example — in `orchestration/dagster/` and one stack — of
chaining jobs with `run_status_sensor`s instead of staggered crons: only the
head job has a schedule; each downstream job fires when its upstream succeeds.
Document the pattern in `docs/architecture-patterns.md`.

**Why.** Staggered crons encode a guess about how long upstream takes. When
upstream runs long, downstream reads stale or half-built data; when it fails,
downstream runs anyway.

**How.** Add to E5's invariants: a job that is chained must have **no** cron of
its own, and every sensor's upstream job must exist.

**Acceptance.** Tests prove the chain: head has a cron, each link has exactly
one upstream sensor and no schedule.

---

### E8. Deployment-routed target database with a fail-safe default

**What.** A small helper that chooses the target database from the deployment
the code is running in (e.g. `DAGSTER_CLOUD_DEPLOYMENT_NAME`, or a
`DEPLOYMENT` env var locally), and records the choice as a plain-text run tag
visible in the UI.

**Why.** "Which database will this write to?" should be a code-reviewed decision,
not infrastructure state. The safety property is the point: **any unknown or
missing deployment name resolves to the non-production database.** Reaching
production requires running under the one recognised production deployment, so
a misconfigured or locally-run process cannot write prod.

**Acceptance.** Unit tests: the prod deployment maps to prod; stage maps to
stage; `None`, `""`, and an unknown name all map to non-prod.

---

## P3 — Warehouse environment management (Snowflake extra)

> These only make sense once a Snowflake RBAC model exists, which is why E9 comes
> first. All macros go under `transformation/dbt/macros/snowflake/` with a
> header saying they are Snowflake-only.

### E9. Snowflake RBAC as code

**What.** Fill `infrastructure/terraform/snowflake/` with a module for the
standard role hierarchy — a per-environment owner role per database, read and
write functional roles, service users on key-pair auth — plus databases,
warehouses, and grants. Widen `terraform-validate.yml` to cover it (this
completes `ci-cd-hardening.md` #14 for Snowflake).

**Why and the rule to encode.** Snowflake future grants have a precedence rule
that bites everyone once: **a schema-level future grant overrides the
database-level future grants for every role on that schema**, not just the role
it names. Add one schema-level future grant and every other role's DB-level
future grants stop applying to new objects in that schema. The module must
therefore either manage future grants at one level only, or push them down to
every schema consistently. Document it in the module README.

Also encode: service users authenticate by key pair, not password, and have an
explicit default role.

**Acceptance.** `terraform validate` and `terraform test` pass; a test asserts
that no schema receives a future grant unless every functional role receives the
matching one.

---

### E10. Zero-copy environment refresh

> **Depends on E9.**

**What.** `clone_database(source, target, copy_grants=true)` and a
`refresh_environment(env)` wrapper, run via `dbt run-operation`, that rebuilds
stage/dev as a zero-copy clone of prod.

**Why.** Stage and dev drift from prod until tests pass for the wrong reasons. A
clone is instant and storage-free until data diverges. `CREATE OR REPLACE`
without `COPY GRANTS` drops explicit grants on the replaced objects, which is
the usual way a refresh silently locks out a downstream role.

**Acceptance.** A dry-run mode logs the DDL; the wrapper refuses to target the
prod database by name.

---

### E11. Preservation manifest for non-dbt objects

> **Depends on E10.**

**What.** A manifest macro listing objects dbt does not manage but the
environment needs — stages, UDFs, event tables, external tables — with their DDL,
grants, and post-create steps, and an engine with three modes: `restore`
(recreate everything after a clone), `provision` (create only what is missing),
and `verify` (read-only report).

**Why.** A clone copies what prod has *right now*. Objects that exist only in
stage — or that must differ per environment — vanish on every refresh, and
nothing notices until a job fails.

**Acceptance.** `verify` reports missing/present per manifest entry without
writing; `restore` after a clone brings `verify` back to all-present.

---

### E12. Ownership drift audit and repair

> **Depends on E9.**

**What.** A read-only `audit_object_ownership(database, owner_role)` reporting
tables, views, and schemas not owned by the database's designated owner role,
plus a missing `CREATE SCHEMA` grant; and `normalize_object_ownership(...)`
that transfers ownership in bulk. The two must agree on what "correct" means.

**Why.** A clone **preserves each object's owner**. Clone prod into stage and the
role that runs stage's dbt jobs owns nothing, so its first `create or replace`
of an existing model fails — as do the `__dbt_tmp` tables incrementals and
snapshots need.

**Acceptance.** Audit after a clone reports drift; normalize then audit reports
none; normalize defaults to dry-run.

---

### E13. Write-boundary proof and identity probe

> **Depends on E9 and E8.**

**What.** Two manual-only Dagster jobs (never scheduled; E5 enforces that):
- **Identity probe** — runs one read-only query on the run worker and logs
  `current_user()`, `current_role()`, `current_secondary_roles()`, database, and
  warehouse.
- **Write-boundary assertion** — attempts to create a uniquely named throwaway
  table in a database the running identity must **not** be able to write, and
  passes only if the warehouse refuses. It logs the refusal verbatim as evidence.

**Why.** Role separation between stage and prod is usually verified by
inference — "the config says stage role, so it must be". Only a refused write,
from inside the container that does the work, proves the whole chain (secret →
env var → profile → resolved role). Secondary roles are part of the pass
condition: an active secondary role can satisfy a privilege check through
inheritance and quietly defeat the boundary.

**Acceptance.** Against a stage deployment, the assertion passes because the
write is refused; if the throwaway table is ever created, the job drops it and
fails.

---

## P4 — Shippable quality tools

### E14. Add a `tooling/` role

**What.** A fourth role, `tooling/`, for developer-side tools a downstream
project adopts alongside a stack: `tooling/_template/README.md` stating what such
a tool provides (a `pyproject.toml`, a pre-commit hook entry and/or a GitHub
Actions snippet, tests that need no network), wired through the `add-tool`
skill. Update the Project Structure section of `CLAUDE.md` and the README tool
table.

**Why.** E15 and E16 have no natural home: they are not EL, orchestration, or
transformation.

**Acceptance.** `add-tool` recognises the role; `just --list` shows the new
modules once E15/E16 land.

---

### E15. Local Claude pre-push review

> **Depends on E14, E16, and `ci-cd-hardening.md` #5.** Complements #10 (CI
> review): this one runs before the push, on the developer's machine.

**What.** `tooling/claude-review/` — an opt-in, advisory Claude review of the
diff about to be pushed, registered as a pre-commit `pre-push` hook and enabled
per push with an env var (`CLAUDE_REVIEW=1 git push`). It prints findings to the
terminal and blocks the push **only** for critical-tier findings.

**How — the invariants that make it safe to ship:**
- **Fail open.** A timeout, a missing CLI, an auth error, or an unparseable
  response prints a warning and allows the push. It must never raise.
- **Gate what leaves the machine.** Run E16's scanner over the diff *before*
  transmitting it; files that trip it are withheld, and the review says so. The
  transmitted set must always be a subset of the scanned set.
- **Bound the payload** — max files, max bytes, and a configurable timeout —
  and say what was truncated.
- **Resolve refs correctly.** pre-commit consumes the hook's stdin, so read the
  push range from the environment pre-commit exposes (`PRE_COMMIT_FROM_REF` /
  `PRE_COMMIT_TO_REF`), and handle new branches, renames, and deletions.
- **Stdlib-only script**, runnable directly with `python3 review.py --help`, so
  it needs no venv on a laptop.
- Parse an explicit verdict line from the model; anything else is "advisory".

**Acceptance.** Tests mock `subprocess` and need no network: fail-open on each
error class; a critical verdict blocks; a withheld file is never transmitted;
limits truncate with a notice; a new-branch push resolves its base.

---

### E16. Configurable sensitive-data commit guard

> **Depends on E14.** This repo's own CI keeps `gitleaks` (`ci-cd-hardening.md`
> #7). E16 is a *tool for downstream projects* whose data is regulated, where
> secrets scanning is not enough.

**What.** `tooling/sensitive-scan/` — a high-precision scanner that runs as a
pre-commit hook and as a required GitHub Action from one script, with pattern
packs selected by config:
- `secrets` — keys and tokens (defer to gitleaks where installed);
- `pii` — SSN-shaped and phone-shaped values, email addresses, a personal name on
  the same line as a date of birth or other attribute;
- `identity-literals` — a literal UUID compared against a configured list of
  identity columns (`where customer_id = '<uuid>'`), and tab- or pipe-separated
  data-dump rows containing an identifier plus a date.

**Rules that keep it honest:**
- Tune for precision; a guard that cries wolf gets bypassed.
- The **only** exemption is a per-line `sensitive-scan:ignore` comment on
  synthetic fixtures. No directory-wide skip lists — they are how real data
  lands in a "tests" folder.
- Ship an incident-response runbook: what to do once sensitive data *has*
  reached git history (rotate, rewrite history, force-push, invalidate caches,
  notify).

**Acceptance.** Fixture tests: each rule trips on its positive example, stays
quiet on a near-miss, and honours the per-line ignore; a directory-level ignore
is rejected by config validation.

---

## P5 — Skills, docs, and optional extras

> Stack skills follow `.claude/skills/CLAUDE.md`: they infer the tool root, never
> assume this repo's layout, and end with **Verify** and **Common mistakes**.

### E17. `data-profiling` stack skill

Use before writing a staging model: profile the raw table for nulls, empty
strings, leading/trailing whitespace, mixed case, distinct-value distributions,
constant columns, numeric outliers, and mixed types — then feed the findings
into `dbt-source` (tests) and `dbt-model` (cleaning). Queries must be generic SQL
with an adapter note, sampled on large tables.

### E18. `dbt-test` stack skill

Generate or update a model's `.yml`: identify the grain first and emit the
matching uniqueness test (`unique` for one column,
`dbt_utils.unique_combination_of_columns` for a composite), doc-block-first
column descriptions, and `not_null`/`relationships`/`accepted_values` only where
the profile supports them. The common mistake to call out: a uniqueness test on
the wrong column set passes while the real grain is broken.

### E19. `backfill-runbook` stack skill

Plan and execute a backfill: scope the models and window, map downstream blast
radius (`dbt ls --select model+`), check source retention before any
`--full-refresh` (see E2), dry-run, run in bounded batches, verify parity
(row counts and a checksum per batch against the pre-backfill state), and write
up what was done.

### E20. State migrations on promotion

Add `docs/patterns/state-migrations.md` and a `migrations/_template.md`: when a
PR changes **physical state** that promoting code does not carry — a seed that
must be reloaded, a subtree that must be rebuilt, an incremental table's column
layout — it ships a migration file stating the per-environment action, who runs
it, and how to verify it. Promotion moves code, not table state; this makes the
gap a reviewed artifact instead of something reconstructed from memory after an
outage.

### E21. Architecture decision records

Add `docs/adr/` with a README (format, numbering, status lifecycle) and seed it
with the decisions this repo already made implicitly: `uv` over pip/poetry;
`just` modules per tool; duckdb as the credential-free default target; `dbt_env`
as a var independent of `target.name`; skills-as-procedure vs
`CLAUDE.md`-as-convention.

### E22. Semantic-view generator from dbt metadata (Snowflake extra)

A macro that emits Snowflake `SEMANTIC VIEW` DDL per mart domain from the dbt
graph — model and column descriptions, data types, and a `meta` flag that
excludes sensitive columns — so natural-language query tools read curated marts
instead of raw tables. Draw the line between what is generated (dimensions,
facts, descriptions) and what must stay hand-curated (metrics, relationships,
synonyms) in the header.

---

## Deliberately left out

Reviewed and not proposed, so they are not re-derived later:

- **Branch promotion-path enforcement** and **release notes published to a
  wiki** — already declined in `ci-cd-hardening.md` (context-dependent section).
- **Legacy batch/step-function job infrastructure** — being retired where it
  was found; Dagster replaces it.
- **Domain-specific macros and tests** (address formatting, phone
  normalisation, gender mapping, business-rule tests) — no generic value.
- **Vendor reference-data release tooling.** The *pattern* — declare a dataset
  profile, set-diff two releases in DuckDB, render a workbook — could become a
  "release-to-release diff" tool later if a second use appears.
- **History reconstruction from CDC landing tables.** Valuable but tied to one
  connector's landing semantics; revisit as a dlt/Airbyte pattern doc if needed.
