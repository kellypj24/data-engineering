# Toolkit Expansion Plan

> **Purpose.** A Claude Code–executable backlog of *features* to add to the
> toolkit: dbt building blocks, data delivery and lineage, Dagster patterns and
> observability, warehouse environments and cost, and shippable quality tools.
> Its sibling,
> [`ci-cd-hardening.md`](./ci-cd-hardening.md), covers CI, supply chain, and
> repo hygiene for this repo itself. If a task is about how *this repo* is
> checked, it belongs there; if it is something a downstream project copies out
> of a tool or stack, it belongs here.
>
> **How to use this (Claude Code).** Pick the highest-priority unchecked task
> from the index, implement it, verify against its **Acceptance** criteria, tick
> its box, and note the PR. One task, one PR.
>
> **Provenance.** Tasks are patterns that have run in a production dbt + Dagster
> platform on Snowflake, generalised. The *why* in each task is the failure the
> pattern was built to prevent. Where a task proposes a **different mechanism**
> from the one that ran in production, it says so in a "New mechanism" note —
> don't treat those parts as proven. Nothing here should name a company, a
> client, a business domain, or a vendor dataset — if a task needs a worked
> example, invent a neutral one (`orders`, `customers`, `recipient`).
>
> **IDs are stable.** Tasks keep their E-number when re-prioritised, so the
> index groups are not in numeric order. `ci-cd-hardening.md` cross-references
> these IDs.
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
- [ ] E28. [Seeds drop and recreate, project-wide](#e28-seeds-drop-and-recreate-project-wide)
- [ ] E29. [Protect durable facts that outlive their source](#e29-protect-durable-facts-that-outlive-their-source)
- [ ] E4. [Tiered-severity validation framework with an audit log](#e4-tiered-severity-validation-framework-with-an-audit-log)
- [ ] E3. [UTC timestamp generic test](#e3-utc-timestamp-generic-test)

**P1 — Data delivery & lineage**

- [ ] E23. [Config-driven file export engine](#e23-config-driven-file-export-engine)
- [ ] E24. [Generated dbt exposures for every delivered file, with a drift gate](#e24-generated-dbt-exposures-for-every-delivered-file-with-a-drift-gate)
- [ ] E25. [Delivery reconciliation (follow-up to E23)](#e25-delivery-reconciliation-follow-up-to-e23)

**P2 — Dagster patterns & observability**

- [ ] E5. [Definition invariant tests](#e5-definition-invariant-tests)
- [ ] E26. [Run-event telemetry and a run-summary model](#e26-run-event-telemetry-and-a-run-summary-model)
- [ ] E6. [One notification model, rendered by severity](#e6-one-notification-model-rendered-by-severity)
- [ ] E8. [Deployment-routed target database with a fail-safe default](#e8-deployment-routed-target-database-with-a-fail-safe-default)
- [ ] E7. [Run-status sensor chaining](#e7-run-status-sensor-chaining)
- [ ] E30. [Versioned dbt docs publishing with a fail-safe destination](#e30-versioned-dbt-docs-publishing-with-a-fail-safe-destination)

**P3 — Warehouse environments & cost (Snowflake extra)**

- [ ] E9. [Snowflake RBAC as code](#e9-snowflake-rbac-as-code)
- [ ] E31. [Personal development databases](#e31-personal-development-databases)
- [ ] E10. [Zero-copy shared-environment refresh](#e10-zero-copy-shared-environment-refresh)
- [ ] E11. [Preservation manifest for non-dbt objects](#e11-preservation-manifest-for-non-dbt-objects)
- [ ] E12. [Ownership drift audit and repair](#e12-ownership-drift-audit-and-repair)
- [ ] E13. [Write-boundary proof and identity probe](#e13-write-boundary-proof-and-identity-probe)
- [ ] E27. [Query-level cost attribution reconciled to the invoice](#e27-query-level-cost-attribution-reconciled-to-the-invoice)

**P4 — Shippable quality tools**

- [ ] E14. [Add a `tooling/` role](#e14-add-a-tooling-role)
- [ ] E15. [Local Claude pre-push review](#e15-local-claude-pre-push-review)
- [ ] E16. [Configurable sensitive-data commit guard](#e16-configurable-sensitive-data-commit-guard)
- [ ] E34. [Release-to-release dataset diff](#e34-release-to-release-dataset-diff)

**P5 — Skills, docs, and optional extras**

- [ ] E17. [`data-profiling` stack skill](#e17-data-profiling-stack-skill)
- [ ] E18. [`dbt-test` stack skill](#e18-dbt-test-stack-skill)
- [ ] E19. [`backfill-runbook` stack skill](#e19-backfill-runbook-stack-skill)
- [ ] E33. [Three-dimensional tagging contract](#e33-three-dimensional-tagging-contract)
- [ ] E32. [Snapshot design guide](#e32-snapshot-design-guide)
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
- **No outbound data path.** Nothing exports files or records run history, and
  no dbt exposures exist.
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

### E28. Seeds drop and recreate, project-wide

**What.** In `dbt_project.yml`, set `+full_refresh: true` on the whole `seeds:`
block, give seeds their own `+persist_docs` (model-level `persist_docs` does not
apply to seeds), and require every seed to have a `.yml` with a description,
an owner in `meta`, and at least one test.

**Why.** dbt's default seed load is truncate-and-insert, and its `INSERT` names
the CSV's columns. The first run after a CSV gains a column fails against the
existing table with `invalid identifier <new column>` — and stays broken until
someone remembers to pass `--full-refresh` by hand. In production this left a
scheduled seed job red for days. A seed is fully defined by its CSV, so
recreating one is always equivalent to reloading it; that is why the setting is
project-wide rather than per-directory.

**Acceptance.** Add a column to a seed CSV and run `dbt seed` without
`--full-refresh`: it succeeds, and the new column is present.

---

### E29. Protect durable facts that outlive their source

> **Pairs with E2.**

**What.** A documented pattern, and one example model, for incremental facts
whose history is **longer than their source's retention** (usage and audit views
that keep 365 days, APIs that page back 90, CDC streams that get refreshed):
- `full_refresh: false` in the model config, so `--full-refresh` cannot rebuild it;
- a bounded restatement window (`var('<model>_restate_periods', 2)`) — only the
  last N periods are ever reprocessed;
- a control-total test that ties each complete period to an independent total,
  and **fails when zero periods are compared**, so an empty join can't pass
  vacuously;
- a header comment stating the retention mismatch, so the next person knows why
  the model refuses a rebuild.

**Why.** A rebuild re-reads the source. Where the source has aged out, it
silently replaces real history with whatever is left — no error, just less or
coarser data.

**Acceptance.** `dbt build --full-refresh --select <model>` leaves existing rows
intact; the control test fails on an empty comparison and passes on a matching
one.

---

### E4. Tiered-severity validation framework with an audit log

**What.** A macro library, `macros/validation/`, for rule sets that are richer
than dbt tests, plus the models it feeds:
- `validate_data_source(source_model, rules)` — `rules` is a dict of
  `{rule_name: {logic: <SQL predicate>, severity: LOW|MEDIUM|HIGH|CRITICAL}}`
  declared in the validation model itself. It emits a pass/fail column and a
  severity column per rule, the **maximum** severity that failed (a numeric rank
  plus its label), and an overall `validation_result` of `PASS`, `WARN`, or `FAIL`.
- `get_validation_config(name)` — merges a per-validation override from
  `var('validation_configs')` over defaults (`enabled`, `lookback_days`,
  `notification_enabled`, `notification_channel`, `retention_days`).
- `should_send_notification(result, severity, name)` — CRITICAL always
  notifies; HIGH notifies on `FAIL`; MEDIUM notifies on `FAIL` only for names in
  `var('high_priority_validations')`; LOW never notifies.
- `get_notification_channel(name, severity)` — severity→channel routing from a var.
- A deterministic key per result row (validation, source table, record id, run
  time), minted with E1.
- One model per rule set, an **incremental `validation_log`** that appends every
  failure and purges rows older than `retention_days`, and a summary model for
  dashboards.

Map the tiers onto dbt severity explicitly in the README: blocking rules use
`error` and stop the pipeline; conditional rules use `error` or `warn` per rule;
quality rules use `warn` and are logged only.

**Why.** dbt tests are binary and ephemeral — they pass or fail and the evidence
is gone. Operations need severities that decide whether a run stops, warns, or
only records; notifications routed by severity; and a failure history so trends
are visible.

**Acceptance.** An example rule set with rules at three severities runs on
duckdb. `validation_log` accumulates across two runs and purges beyond retention.
A singular test over literal inputs covers every branch of
`should_send_notification`. Changing `validation_configs` changes behaviour with
no code change.

---

### E3. UTC timestamp generic test

**What.** `tests/generic/timestamp_is_utc.sql` — fails rows whose timestamp column
carries a non-UTC offset. Dispatch per adapter; on adapters without offset-aware
types, document it as a no-op rather than silently passing.

**Why.** Mixed-offset timestamps are the classic silent join bug between two
sources that agree on the wall clock but not the zone.

**Acceptance.** Documented in the dbt tool's README and used once in the example
project; `dbt parse` and `dbt compile` stay green.

---

## P1 — Data delivery & lineage

### E23. Config-driven file export engine

**What.** A declarative export pipeline: one YAML per export, one engine that
turns it into SQL, writes the files, keeps a run log, and generates its own
schedules. Put the core (loader, SQL builder, executor) in a package with **no
orchestrator imports**, wrapped by a CLI and a Dagster op, so the Airflow and
Prefect tools can wrap it later.

**The config contract.**
- **Two export kinds.** *Shared*: one model serves every recipient, filtered per
  recipient by a tenant key listed in the YAML. *Dedicated*: one model per
  recipient with filters baked in; the YAML carries delivery settings only. Plus
  *ad hoc*: any relation, never scheduled.
- **Filters**: `in`, `not_in`, `ilike`, `not_null`, and range filters whose
  boundary semantics are in the key name — `start_inclusive_end_inclusive`
  (`[start, end]`) and `start_exclusive_end_inclusive` (`(start, end]`) — with date
  keywords such as `today`, `last_sunday`, `max_value`, and `last_run_end`.
- Column selection with aliases; computed columns with `{range_start}` /
  `{range_end}` placeholders; boolean values emitted unquoted.
- **Multiple outputs**: one query, several destinations or formats.
- Optional post-processing: header/trailer records, and a control/manifest file
  next to the data file.
- An optional `schedule:` block per recipient.

**The engine.**
- **Three runner modes**: `dry-run` prints SQL and has no side effects;
  `select-only` runs the SELECT and reports the row count; `execute` writes
  (`COPY INTO`/`UNLOAD`, or `COPY ... TO` on duckdb).
- **Run log and watermark.** Write a run-log row after every attempt, success or
  failure. `last_run_end` resolves to the last successful window's end, and
  incremental windows are `(start, end]`: the row *at* the watermark was sent
  last time. Four edge cases to encode and document:
  1. A 0-row run is a success and **advances** the watermark, so idle days don't
     stall it.
  2. A first run with no seeded watermark exports all history. Cutover from a
     legacy job means inserting one seed row taken from the legacy job's last
     window *at cutover time*.
  3. Read both window bounds from the same snapshot, so a stale upstream causes
     lag, not loss.
  4. `max_value` ignores row filters. An incremental export with an `in` filter
     can advance past rows it excluded — warn when both are configured.
- **Tenant isolation fails closed.** Before the first write for a recipient of a
  shared export, count rows outside the expected tenant keys, and nulls. Any
  count above zero raises. No file is written, the remaining outputs for that
  recipient are skipped (no partial delivery), and the error goes to the run log.
  At parse time, a shared-export config without tenant keys raises before any
  write, on every path (dry-run, CLI, and orchestrator).
- **Schedules are generated.** Each recipient's `schedule:` block becomes one job
  and one schedule, so adding a scheduled export is a YAML edit. Generated
  definitions must pass E5's invariants (default `STOPPED`, UTC, unique names).

**Why.** Hand-written export jobs drift: every file gets its own SQL, its own
windowing bug, and its own idea of "incremental". One contract makes a delivery
reviewable as data. The tenant check exists because the most expensive failure an
export can have is sending one recipient another recipient's rows.

> **New mechanism:** in production the "every config validates" test ran in a
> non-gating job, so a broken config was caught only at runtime. Here it must
> be a merge gate.

**Acceptance.** Tests against duckdb:
- dry-run writes nothing;
- a three-run watermark chain, including a 0-row run, produces contiguous
  windows;
- a fixture with one foreign-tenant row fails closed with no file written;
- a shared config missing tenant keys raises at parse time;
- a test auto-discovers every YAML under the configs directory, validates it, and
  runs in the required CI job.

---

### E24. Generated dbt exposures for every delivered file, with a drift gate

> **Depends on E23.**

**What.** A small package that reads E23's configs and writes
`models/exports/_generated_exposures.yml`: one dbt exposure per delivered file,
with a deterministic name and label, `depends_on` the model it reads, and an
owner. It cross-checks the manifest: the model exists, aliases resolve, any
columns the config pins exist in the model, and no two exposures share a name.
The CLI has `--write` and `--check`. `--check` runs after `dbt parse` and needs no
warehouse connection. Wire `--check` into pre-commit and a CI job.

**Why.** Without it, a file sent outside the company is invisible in the DAG, and
`dbt ls --select +exposure:*` can't answer "what does this PR change for the
people we send files to?". The drift gate matters as much as the generator:
a stale generated file is worse than none, because it looks authoritative.

**Acceptance.**
- Adding a config without regenerating fails `--check`.
- A config pointing at a model that doesn't exist fails.
- A pinned column the model lacks fails.
- The generator's tests run offline against a fixture manifest.

---

### E25. Delivery reconciliation (follow-up to E23)

> **Depends on E23.** Do it after E23 has real users.

**What.** A model that compares **expected** deliveries (from E23's schedules and
run log) with **confirmed** transfers (whatever the transport records: SFTP
server logs, object-store events, API acknowledgements) and classifies each as
`delivered`, `late`, `missing`, or `unexpected`.

**Why.** A successful run-log row proves a file was *written*, not that it
*arrived*. Recipients notice missing files before engineers do.

**Not in scope:** parsing any particular transport's log format. That parsing is
per-transport glue; the portable part is the expected-versus-confirmed
contract.

**Acceptance.** Seeded expectations and confirmations on duckdb produce each of
the four states.

---

## P2 — Dagster patterns & observability

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

Persisting run events to the warehouse is a separate task, E26. Keep the two
decoupled: the notifier renders, telemetry records.

**Why.** Per-job ad hoc Slack messages drift until nobody reads them. Separating
rendering from telemetry means an Airbyte or file-transfer job reuses the same
alert standard by building a `Notification`, without touching the telemetry
schema. Routing in one place makes "split warnings into their own channel" a
one-line change.

**Acceptance.** Existing hook call sites use the new module; unit tests render
each severity to Block Kit JSON and snapshot them.

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

### E26. Run-event telemetry and a run-summary model

> **Pairs with E6.**

**What.**
- **An event table** (committed DDL): run id, job name, status (`STARTED`,
  `SUCCESS`, `FAILURE`), trigger source (schedule, sensor, manual), dbt command,
  warehouse, timestamp, and test counts.
- **Writers**: success and failure hooks, plus a start event, append rows. **Every
  telemetry error is swallowed**, so observability can never fail a pipeline.
- **A safety-net sensor** writes the missing `STARTED` or `FAILURE` row when the
  framework dies before a hook fires.
- **In dbt**: a staging model over the event table, and
  `mart_orchestrator_run_summary` with one row per run. It pairs the first
  `STARTED` with its completion on run id. Runs without a completion yet appear as
  `IN_PROGRESS` with null completion metrics. Columns: duration, `is_success` /
  `is_failure` flags, test count, test failures, failure rate, and date parts
  (day, week, month, hour, day of week). Exclude the job that refreshes the
  monitoring models from its own summary.

**Why.** The orchestrator's UI answers "what happened to this run". It does not
answer "is this job getting slower", "which jobs fail most on Mondays", or "how
many tests failed last month". That needs run history as data, joinable to
everything else in the warehouse.

**Acceptance.** Seeded events on duckdb produce correct rows for a success, a
failure, an in-progress run, and a run with duplicate `STARTED` events (the first
one wins). A raising telemetry writer does not fail the job under test.

---

### E30. Versioned dbt docs publishing with a fail-safe destination

> **Depends on E8.**

**What.** A scheduled job that runs `dbt docs generate` and packages the static
site. It stamps each build with immutable version metadata (git SHA, build time,
dbt version) and uploads to object storage. The destination comes from the
deployment, E8-style: prod publishes to the live site; stage **and any unknown
deployment** publish to a test destination. Publishing to live from anywhere else
is refused unless an explicit allow flag is set.

**Why.** Hand-published docs go stale, and nobody can tell which commit they
describe. A process that can reach the live site from a local run will
eventually overwrite it with a half-built branch.

**Acceptance.** Unit tests cover routing for prod, stage, unknown, and missing
deployments. The live destination is refused without the flag. The metadata file
is present in the packaged site.

---

## P3 — Warehouse environments & cost (Snowflake extra)

> These only make sense once a Snowflake RBAC model exists, which is why E9 comes
> first. All macros go under `transformation/dbt/macros/snowflake/` with a
> header saying they are Snowflake-only.

### E9. Snowflake RBAC as code

**What.** Fill `infrastructure/terraform/snowflake/` with a module for the
standard role hierarchy — a per-environment owner role per database, read and
write functional roles, service users on key-pair auth — plus databases,
warehouses, and grants. Widen `terraform-validate.yml` to cover it (this
completes `ci-cd-hardening.md` #14 for Snowflake).

> **New mechanism:** in production, roles and grants were versioned SQL DDL
> scripts, applied by hand. Terraform is this toolkit's choice and is untested
> in production. The precedence rule below *is* production-learned. If Terraform
> proves awkward, ship the DDL-script form instead, with the same test.

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

### E31. Personal development databases

> **Depends on E10's `clone_database`.** Split out of E10 because the safety
> rules differ: this one runs on every engineer's laptop.

**What.**
- `refresh_dev_database(username=none)` — a `run-operation` that defaults
  `username` to `current_user()` and clones prod into `<PROD_DB>_<USERNAME>`.
  Accept an explicit override for named experiments.
- The dev target in `profiles.yml` defaults its database to that
  identity-derived name, and its schema to `<USER>_DEV`, so a fresh clone
  needs no configuration.
- The prod target has **no defaults at all**: every connection value comes from
  an env var with no fallback, so a missing variable fails loudly rather than
  quietly resolving to something.
- A `just dbt::dev-database` recipe.

**Why.** Shared dev databases mean engineers overwrite each other's work, and
"works on my branch" stops meaning anything. A personal zero-copy clone takes
minutes, costs nothing until data diverges, and needs no ticket. Defaults are
safe on the dev target and dangerous on the prod one.

**Acceptance.** The macro's dry run prints the source and derived target. The
prod target fails to resolve when an env var is unset. Running the recipe twice
replaces the clone without error.

---

### E10. Zero-copy shared-environment refresh

> **Depends on E9.** Personal clones are E31.

**What.** `clone_database(source, target, copy_grants=true)` and a
`refresh_environment(env)` wrapper, run via `dbt run-operation`, that rebuilds
shared stage/test environments as zero-copy clones of prod, on a schedule.

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

### E27. Query-level cost attribution reconciled to the invoice

> **Depends on E29** (the monthly fact outlives its sources). Snowflake extra.

**What.** Source models over `SNOWFLAKE.ACCOUNT_USAGE` / `ORGANIZATION_USAGE`,
declared read-only, feeding a monthly cost fact by team and workload:
- **Attribute compute by query** (query attribution history joined to query
  history), not by warehouse name.
- Put **idle time and cloud-services** credits in an explicit `SHARED` bucket
  instead of spreading them across teams. Attribute storage by owning database.
- **Two mapping seeds** hold the attribution vocabulary: user → team/workload and
  database → team. A narrow "resolve by database" sentinel covers genuinely
  shared service accounts only. It must not be the default for service accounts.
- Anything unmapped lands in an explicit `NEEDS_OWNER_REVIEW` bucket, never in a
  guess.
- **Conformed dimensions** (month, team, workload, warehouse) are built from the
  spine and seeds, **never from the fact**, or their relationships tests could
  never fail. The fact's foreign keys are `not_null` even where the natural
  column is nullable, because a NULL FK passes a relationships test silently. The
  warehouse dimension is accumulating, because its sources keep 365 days while
  the fact keeps months forever.
- **A control-total test** ties every complete month to the invoiced amount in
  currency, within a cent.
- An optional per-unit denominator hook (`var`) gives cost per customer, order,
  or other unit.

**Why.** Warehouse-name attribution is the obvious first cut, and it's wrong in
both directions. In production it overstated one team's share by about 1.6×. A
shared warehouse named after a consumer can be entirely another team's work,
while a shared service account can hide the biggest spender. Total spend doesn't
move; only its ownership does, and ownership is the question being asked.
Isolating ~40% as `SHARED` is honest; spreading it across teams invents precision.

**Acceptance.** On a Snowflake account, every complete month ties to the invoice
within a cent. The `NEEDS_OWNER_REVIEW` share is reported, not hidden.
Relationship tests pass with every FK `not_null`.

---

## P4 — Shippable quality tools

### E14. Add a `tooling/` role

**What.** A fourth role, `tooling/`, for developer-side tools a downstream
project adopts alongside a stack: `tooling/_template/README.md` stating what such
a tool provides (a `pyproject.toml`, a pre-commit hook entry and/or a GitHub
Actions snippet, tests that need no network), wired through the `add-tool`
skill. Update the Project Structure section of `CLAUDE.md` and the README tool
table.

**Why.** E15, E16, and E34 have no natural home: they are not EL, orchestration, or
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

### E34. Release-to-release dataset diff

> **Depends on E14.**

**What.** `tooling/release-diff/` — compare two captures of the same dataset (a
vendor's monthly release, two snapshots of a reference table, prod against a
rebuilt candidate) and report rows **added**, **removed**, and **changed**:
- **Dataset profiles** are committed Python/YAML declaring key columns, compare
  columns, expected headers, and sort order.
- **A pure-logic core**, stdlib-only with no I/O, builds the SQL from a profile,
  validates headers, resolves which pair of captures to compare, and computes
  the summary. DuckDB executes the set difference.
- **Invariants are asserted, not assumed**: `new_rows − old_rows = added −
  removed`, and every changed row's key exists on both sides.
- Output as CSV and markdown first; a workbook renderer can come later.
- **Trust model, stated in the header**: every *identifier* interpolated into SQL
  comes from a committed profile, and every *value* is escaped. Static analysers
  will flag the builders, and the header explains why that's safe.

**Why.** "What changed in this release?" comes up for every external dataset and
every risky rebuild, and ends up answered by ad hoc `EXCEPT` queries that nobody
checks for arithmetic consistency.

**Acceptance.** Fixture captures produce the expected added, removed, and changed
counts. A deliberately inconsistent fixture trips the invariant. A header
mismatch fails before any diff runs.

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

### E33. Three-dimensional tagging contract

Tag models along three independent axes so selections compose instead of
multiplying:
- **Layer**, set automatically by directory in `dbt_project.yml` (`staging`,
  `intermediate`, `marts`, …), with no manual tagging;
- **Workload** or business area, set manually in the model's `.yml`;
- **Entity**, the domain object the model represents (`customer`, `order`), also
  manual.

Then `--select tag:orders,tag:staging` works, and production jobs select by
tag instead of by path. Ship an ADR (E21), a tagging reference doc, and an
enforcement check on `manifest.json`: every model has exactly one layer tag, and
every tag comes from an allowed list (a var or seed). Without enforcement the
taxonomy decays into a flat tag soup within months. **Acceptance:** a model with
an unlisted tag or two layer tags fails the check.

### E32. Snapshot design guide

Add `docs/patterns/snapshots.md` and one example snapshot on the example
project, covering:
- **Strategy**: `timestamp` when the source has a trustworthy `updated_at`;
  `check` with a stable, explicit `check_cols` list otherwise.
- **Hard deletes**: the `hard_deletes` config, and what each option means
  downstream.
- **The point-in-time query**:
  `valid_from <= t and (t < valid_to or valid_to is null)`.
- **Cadence**: schedule snapshots shortly *before* the jobs that read them.
- **The backfill expectation, stated plainly**: history begins at the first run.
  A snapshot of a current-state source cannot reconstruct the past, so start
  snapshotting before anyone needs the history.

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
- **Transport-specific log parsing** (SFTP server logs and similar). Format
  glue, not a pattern — E25 keeps only the expected-versus-confirmed contract.
- **Import-CTE helper macros.** Cosmetic; a `dbt-model` skill convention covers
  it without a macro.
- **History reconstruction from CDC landing tables.** Valuable but tied to one
  connector's landing semantics; revisit as a dlt/Airbyte pattern doc if needed.
