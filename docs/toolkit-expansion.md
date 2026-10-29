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

- [x] E1. [Surrogate-key minting macro](#e1-surrogate-key-minting-macro)
- [x] E2. [In-place surrogate-key backfill operation](#e2-in-place-surrogate-key-backfill-operation)
- [x] E28. [Seeds drop and recreate, project-wide](#e28-seeds-drop-and-recreate-project-wide)
- [x] E29. [Protect durable facts that outlive their source](#e29-protect-durable-facts-that-outlive-their-source)
- [x] E4. [Tiered-severity validation framework with an audit log](#e4-tiered-severity-validation-framework-with-an-audit-log)
- [x] E3. [UTC timestamp generic test](#e3-utc-timestamp-generic-test)

**P1 — Data delivery & lineage**

- [x] E23. [Config-driven file export engine](#e23-config-driven-file-export-engine)
- [x] E24. [Generated dbt exposures for every delivered file, with a drift gate](#e24-generated-dbt-exposures-for-every-delivered-file-with-a-drift-gate)
- [x] E25. [Delivery reconciliation (follow-up to E23)](#e25-delivery-reconciliation-follow-up-to-e23)

**P2 — Dagster patterns & observability**

- [x] E5. [Definition invariant tests](#e5-definition-invariant-tests)
- [x] E26. [Run-event telemetry and a run-summary model](#e26-run-event-telemetry-and-a-run-summary-model)
- [x] E6. [One notification model, rendered by severity](#e6-one-notification-model-rendered-by-severity)
- [x] E8. [Deployment-routed target database with a fail-safe default](#e8-deployment-routed-target-database-with-a-fail-safe-default)
- [x] E7. [Run-status sensor chaining](#e7-run-status-sensor-chaining)
- [x] E30. [Versioned dbt docs publishing with a fail-safe destination](#e30-versioned-dbt-docs-publishing-with-a-fail-safe-destination)

**P3 — Warehouse environments & cost (Snowflake extra)**

- [x] E9. [Snowflake RBAC as code](#e9-snowflake-rbac-as-code)
- [x] E31. [Personal development databases](#e31-personal-development-databases)
- [x] E10. [Zero-copy shared-environment refresh](#e10-zero-copy-shared-environment-refresh)
- [x] E11. [Preservation manifest for non-dbt objects](#e11-preservation-manifest-for-non-dbt-objects)
- [x] E12. [Ownership drift audit and repair](#e12-ownership-drift-audit-and-repair)
- [x] E13. [Write-boundary proof and identity probe](#e13-write-boundary-proof-and-identity-probe)
- [x] E27. [Query-level cost attribution reconciled to the invoice](#e27-query-level-cost-attribution-reconciled-to-the-invoice)

**P4 — Shippable quality tools**

- [x] E14. [Add a `tooling/` role](#e14-add-a-tooling-role)
- [x] E15. [Local Claude pre-push review](#e15-local-claude-pre-push-review)
- [x] E16. [Configurable sensitive-data commit guard](#e16-configurable-sensitive-data-commit-guard)
- [x] E34. [Release-to-release dataset diff](#e34-release-to-release-dataset-diff)

**P5 — Skills, docs, and optional extras**

- [x] E17. [`data-profiling` stack skill](#e17-data-profiling-stack-skill)
- [x] E18. [`dbt-test` stack skill](#e18-dbt-test-stack-skill)
- [x] E19. [`backfill-runbook` stack skill](#e19-backfill-runbook-stack-skill)
- [x] E33. [Three-dimensional tagging contract](#e33-three-dimensional-tagging-contract)
- [x] E32. [Snapshot design guide](#e32-snapshot-design-guide)
- [x] E20. [State migrations on promotion](#e20-state-migrations-on-promotion)
- [x] E21. [Architecture decision records](#e21-architecture-decision-records)
- [x] E22. [Semantic-view generator from dbt metadata (Snowflake extra)](#e22-semantic-view-generator-from-dbt-metadata-snowflake-extra)

### Round 3 (drafted 2026-10-28)

Work order across both roadmaps: **P6 fixes**, then the CI quick wins in
`ci-cd-hardening.md` (#21, #22, #23, #13, #12, in that order), then P7–P9.

**P6 — Fixes to shipped code**

- [x] E35. [claude-review: scan the base side of every diff](#e35-claude-review-scan-the-base-side-of-every-diff)
- [x] E36. [claude-review: run the model in an isolated, non-persistent session](#e36-claude-review-run-the-model-in-an-isolated-non-persistent-session)
- [x] E37. [Dagster: resolve target routing at run time, not import time](#e37-dagster-resolve-target-routing-at-run-time-not-import-time)

**P7 — dbt building blocks, round 2**

- [x] E40. [Row content hash for snapshot `check_cols`](#e40-row-content-hash-for-snapshot-check_cols)
- [x] E39. [Generic SCD2 invariants test](#e39-generic-scd2-invariants-test)
- [x] E38. [SCD2 reconstruction from dated full extracts](#e38-scd2-reconstruction-from-dated-full-extracts)
- [x] E41. [Grain guard: count before dedup](#e41-grain-guard-count-before-dedup)
- [x] E42. [Date spine with a fact-coverage test](#e42-date-spine-with-a-fact-coverage-test)
- [x] E43. [Static check for undefined `var()` references](#e43-static-check-for-undefined-var-references)
- [x] E44. [Pre-run write-role guard (Snowflake extra)](#e44-pre-run-write-role-guard-snowflake-extra)

**P8 — Dagster patterns, round 2**

- [x] E45. [Restricted-tag reach invariant](#e45-restricted-tag-reach-invariant)
- [x] E46. [Per-deployment schedule times](#e46-per-deployment-schedule-times)
- [x] E47. [SQS sensor with redelivery-safe acknowledgement](#e47-sqs-sensor-with-redelivery-safe-acknowledgement)
- [x] E48. [Warehouse backup as an asset](#e48-warehouse-backup-as-an-asset)
- [x] E49. [Read-only Dagster status CLI](#e49-read-only-dagster-status-cli)

**P9 — Pattern docs**

- [x] E50. [Period close](#e50-period-close)
- [x] E51. [History tables stamped with `current_date`](#e51-history-tables-stamped-with-current_date)
- [x] E52. [One unknown-member token per dimension](#e52-one-unknown-member-token-per-dimension)
- [x] E53. [Event-driven ingest → dbt subgraph → export](#e53-event-driven-ingest--dbt-subgraph--export)
- [x] E54. [Image promotion runbook](#e54-image-promotion-runbook)
- [x] E55. [Sensitive-scan runbook: freeze, verify, audit](#e55-sensitive-scan-runbook-freeze-verify-audit)

### Round 4 (drafted 2026-10-29)

Work order: E56–E59, then E60–E70 in number order, then E71–E74. E60–E62 set
the vocabulary (source type, key role, hash) the later tasks use.

**P10 — Fixes to shipped code, round 2**

- [x] E56. [Cost dates truncated in UTC](#e56-cost-dates-truncated-in-utc)
- [ ] E57. [Dagster: Airbyte assets from `AirbyteWorkspace`](#e57-dagster-airbyte-assets-from-airbyteworkspace)

**P11 — Cheap guards**

- [ ] E58. [Fail on ignored `check` constraints (Snowflake extra)](#e58-fail-on-ignored-check-constraints-snowflake-extra)
- [ ] E59. [Definition invariant: scheduled dbt jobs use `build`](#e59-definition-invariant-scheduled-dbt-jobs-use-build)

**P12 — Source contracts, keys and history**

- [ ] E60. [Source capability classification](#e60-source-capability-classification)
- [ ] E61. [Incremental patterns per source type](#e61-incremental-patterns-per-source-type)
- [ ] E62. [Key roles and the identity, content and link hashes](#e62-key-roles-and-the-identity-content-and-link-hashes)
- [ ] E63. [Exclusion signals](#e63-exclusion-signals)
- [ ] E64. [Test ladder](#e64-test-ladder)
- [ ] E65. [Managed history from an observation log](#e65-managed-history-from-an-observation-log)
- [ ] E66. [Snapshot migration hazards](#e66-snapshot-migration-hazards)
- [ ] E67. [CDC landing tables: current state and history](#e67-cdc-landing-tables-current-state-and-history)

**P13 — Project-wide tooling**

- [ ] E68. [dbt baseline census](#e68-dbt-baseline-census)
- [ ] E69. [PR impact checker](#e69-pr-impact-checker)
- [ ] E70. [Consumer blast-radius audit (Snowflake extra)](#e70-consumer-blast-radius-audit-snowflake-extra)

**P14 — Environments, identity and runbooks**

- [ ] E71. [Clone hygiene (Snowflake extra)](#e71-clone-hygiene-snowflake-extra)
- [ ] E72. [Password to key-pair cutover runbook](#e72-password-to-key-pair-cutover-runbook)
- [ ] E73. [Postgres replication-slot guard](#e73-postgres-replication-slot-guard)
- [ ] E74. [`validate-on-clone` stack skill](#e74-validate-on-clone-stack-skill)

Not yet tasks: [Deferred to a later round](#deferred-to-a-later-round).

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

> **New mechanism:** in production the writers were op success/failure hooks
> plus a separate safety-net sensor. The toolkit uses Dagster run-status
> sensors as the writers instead: they fire per run rather than per op, and
> they see a run whose process died, because Dagster marks it FAILURE. The
> safety net becomes the completion writer backfilling a missing STARTED row.
> Test counts come from the run's asset-check evaluations, which is how
> dagster-dbt reports dbt tests.

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
warehouses, and grants. Add a `ci.yml` Terraform job for it, like `test-airbyte`, (this
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

## P6 — Fixes to shipped code

> Defects in tools this repo already ships. Each one leaks data or writes to
> the wrong place, so they come before any new feature or CI polish.

### E35. claude-review: scan the base side of every diff

> **Fixes E15.**

**Why.** `build_payload` scans `git show <head>:<path>` and then sends
`git diff <base> <head> -- <path>`. A diff carries both sides: the removed
lines come from the base version, which is never scanned. A push that deletes a
secret, or rewrites the line it sat on, sends that secret to the model. The
existing invariant (`sent ⊆ scanned`) holds by path but not by content.

**How.**
- For every file whose status is M, R, or D, also scan `git show <base>:<old
  path>`. Parse the old path from `--name-status` (`R100\told\tnew`); today
  `changed_files` keeps only the new path.
- A file is sendable only if **both** sides are clean. Either side tripping the
  scanner withholds the file, and the note says which side.
- Deleted files are still not sent, but they are scanned, so the withheld list
  reports a deleted secret instead of staying silent.
- Scan before any byte is assembled into the payload, not between files.
- Restate the invariant in `CLAUDE.md`: every line in the payload came from a
  scanned blob.

**Acceptance.** Real-repo tests in `tests/test_review.py`: a push that removes
a secret line withholds the file; a rename whose old content trips the scanner
withholds it; a delete of a tripping file appears in the withheld note; a clean
modify still sends. `FakeClaude` records its input, and no test payload ever
contains the fixture secret.

---

### E36. claude-review: run the model in an isolated, non-persistent session

> **Fixes E15.**

**Why.** `ask_claude` runs a bare `claude -p`. That session inherits whatever
the developer's environment configures: tools (it can read and run things),
MCP servers, hooks, and the project `CLAUDE.md`. It also writes a session
transcript, which is a second copy of the diff on disk. A reviewer needs none
of that; it needs the prompt and nothing else.

**How.**
- Build the argv in one function: no tools, a strict empty MCP config, safe
  mode (no hooks, no project or user settings, no `CLAUDE.md`), and no session
  persistence. Use the flags the installed CLI documents; a test pins the argv.
- `CLAUDE_REVIEW_MODEL` overrides the model. Unset means the CLI default.
- Every fail-open warning starts with `UNAVAILABLE (NOT A PASS):`, so a review
  that did not run can never be read as one that passed. A real `VERDICT: OK`
  keeps its own wording.
- If the CLI rejects an isolation flag (older version), fail open with that
  prefix. Never retry without the flag.

**Acceptance.** A test asserts the exact argv, including the model override. A
test per fail-open class asserts the `UNAVAILABLE (NOT A PASS)` prefix. An
unknown-flag error from the CLI fails open and does not re-run `claude`.

---

### E37. Dagster: resolve target routing at run time, not import time

> **Fixes E8.** Uses E13's identity probe.

**Why.** `resources/connections.py` calls
`target_database(current_deployment())` when the module is imported. If the
deployment variable is not in the code server's environment at load time, the
resource is built with the non-prod database, and every prod run then writes
there. The fail-safe default makes this silent: nothing errors, prod just
stops getting data. This happened in production.

**How.**
- Resolve routing inside the op/asset body, immediately before the warehouse
  connection or the `dbt` CLI call, from the run's environment. The dbt call
  passes the resolved database explicitly (env var or `--vars`), never relying
  on the profile's default.
- Import-time `deployment_tags` stay, labelled display-only in the docstring.
  Nothing may route on them.
- Add the warehouse as a **logical size key** (`small`, `large`) mapped per
  deployment in `utils/deployment.py`, with the same fail-safe: an unknown
  deployment gets the non-prod mapping.
- E13's identity probe asserts that the database and warehouse the session
  reports equal what `utils/deployment.py` resolves for the running
  deployment, and fails otherwise.

**Acceptance.** A test imports `connections` with the deployment unset, then
sets it to `prod` and executes the asset: the resolved database is prod. The
size-key mapping has the same unset/empty/unknown → non-prod tests as E8. The
probe fails when the resolved and reported databases differ.

---

## P7 — dbt building blocks, round 2

### E40. Row content hash for snapshot `check_cols`

> **Depends on E1** (reuses its encoding). Used by E38.

**What.** `row_content_hash(relation_or_columns, exclude=[])` in
`macros/utils/`, returning one hash over a row's content, for use as a single
snapshot `check_cols` column.

**Why.** A long `check_cols` list drifts from the table as columns are added.
`check_cols='all'` treats volatile columns (`last_updated`, load timestamps)
as changes and opens a version on every run. Array columns report a change
whenever element order changes.

**How.**
- Reuse `mint_surrogate_key`'s length-framed payload (factor the encoding out
  into a shared macro; do not copy it), so NULL and `''` stay distinct and
  delimiters cannot collide.
- `exclude` removes volatile columns by name. Columns are taken in a fixed,
  sorted order so a column reorder is not a change.
- Arrays are sorted before encoding.
- **Snowflake extra**, in `macros/snowflake/`: ARRAY/VARIANT columns are not
  comparable with `=`, so serialise them to a canonical string before
  hashing. The duckdb implementation is the default dispatch.

**Acceptance.** A singular test on literal rows fails if: changing an excluded
column changes the hash; reordering an array changes it; NULL and `''` hash the
same; reordering columns changes it. Runs on duckdb.

---

### E39. Generic SCD2 invariants test

**What.** `tests/generic/scd2_invariants.sql`
(`scd2_invariants(key, valid_from, valid_to)`) asserting, per key:
- no zero-length spans (`valid_from < valid_to` when closed);
- at most one open version (`valid_to is null`);
- no overlapping spans.

**Why.** Snapshot and reconstructed history tables break in exactly these
three ways, and a point-in-time query over a broken table returns zero rows or
two rows for one key without erroring. `unique` on the surrogate key catches
none of them.

**Acceptance.** Applied to `customers_snapshot`. Fixture seeds with one
violation each fail the test; a clean fixture passes. Runs on duckdb.

---

### E38. SCD2 reconstruction from dated full extracts

> **Depends on E40 and E39.** Distinct from CDC-landing-table reconstruction
> (E67): this reads archived full extracts and depends on no connector.

**What.** `reconstruct_scd2(extracts, key, extract_date, hash_col)` — a macro
that rebuilds type-2 history from a stack of archived full extracts (one row
per key per extract date) using gaps-and-islands.

**Why.** E32 says history begins at the first snapshot run. Often it does not
have to: archived full extracts from before that run already contain the
history. Naive reconstruction compares consecutive hashes only, so a key that
disappears and later returns with the same content looks continuous, and a key
missing from the latest extract stays open forever.

**How.**
- **Presence-aware.** Build the full calendar of extract dates. A key absent
  from an extract closes its version at that extract's date. A key that
  reappears opens a new version, even if its hash is unchanged.
- Islands are runs of consecutive extract dates with the same hash *and*
  continuous presence.
- Output columns match dbt snapshot meta columns (`dbt_valid_from`,
  `dbt_valid_to`, `dbt_scd_id`) so downstream point-in-time queries do not
  change at the seam.
- Extend `docs/patterns/snapshots.md`: start a go-forward snapshot from the
  rebuilt history at a fixed cutover date. The last reconstructed extract and
  the first snapshot run must use the same E40 hash; a test compares hashes of
  open versions at the cutover and fails on any mismatch. Amend "history
  begins at the first run" to point here.

**Acceptance.** A fixture of dated extracts covering change, disappearance,
reappearance-unchanged, and still-present keys produces the expected versions.
E39 passes on the output. The seam test fails when the cutover hash differs.
Runs on duckdb.

---

### E41. Grain guard: count before dedup

**What.** `grain_guard(grain, order_by)` in `macros/utils/`: dedups to the
declared grain with `row_number()` and keeps `count(*) over (partition by
<grain>)` as a `_grain_rows` column. A generic test,
`grain_guard_is_one`, at `severity: warn`, checks `_grain_rows <= 1`.

**Why.** Deduping to a grain makes `unique` pass by construction. A real key
collision upstream (two source rows claiming one `order_id`) becomes an
arbitrary pick, and every test stays green.

**How.**
- `order_by` is the tie-break. Its header says volatile columns
  (`_loaded_at`, `last_updated`) must not be in it, or the chosen row changes
  run to run and snapshots open spurious versions.
- Snapshots read the guarded model with `select * exclude (_grain_rows)`, so
  the count never becomes part of tracked history.
- Wire it into one example staging model.

**Acceptance.** A fixture with a duplicated key emits a warning and still
builds one row; a clean fixture is silent. The example snapshot's columns do
not include `_grain_rows`.

---

### E42. Date spine with a fact-coverage test

**What.** A `dim_date` built on `dbt_utils.date_spine` with start and end from
vars, plus a generic test `within_date_spine(date_column)` that fails when a
fact date falls outside the spine.

**Why.** Dates outside the spine do not error; they fail to join and the fact
rows silently drop out of anything joined through `dim_date`. Spines are
usually set once, too narrow, and never revisited.

**How.** Document in the model header that `date_spine`'s `end_date` is
**exclusive**, the usual off-by-one. Apply the test to the example facts.

**Acceptance.** A fixture fact dated past the spine end fails the test; moving
the end var forward one day passes it. Runs on duckdb.

---

### E43. Static check for undefined `var()` references

**What.** `.github/scripts/check_dbt_vars.py` (stdlib) plus a CI step and a
`just dbt::check-vars` recipe: flag every `var('x')` with no default whose name
is not defined under `vars:` in `dbt_project.yml`.

**Why.** `dbt parse` does not render model bodies, so a removed or renamed var
surfaces only at compile. Downstream projects often compile only against a
real warehouse, so the break reaches main.

**How.**
- Allow the defaulted form `var('x', <default>)`.
- Lex Jinja string literals properly: quotes inside strings, both quote
  styles, `var` inside comments. No regex over raw text.
- Scan `models/`, `macros/`, `snapshots/`, `tests/`, and property YAML.
- Keep the post-merge compile gate (`ci-cd-hardening.md` #18): two green PRs
  can still break main together.

**Acceptance.** Tests: an undefined bare var fails with file and line; a
defaulted var passes; `var` inside a string or Jinja comment is ignored; a var
defined only under a package scope is handled. The example project passes.


Done: lives in the dbt tool as `transformation/dbt/scripts/check_vars.py` (not
`.github/scripts/`): it ships with the stack, so downstream projects that copy
`transformation/dbt/` get it. `just dbt::check-vars`; CI runs it through
`tests/python/test_check_vars.py` in the existing `test-dbt` job. Uses Jinja's
own lexer from the dbt venv instead of a stdlib lexer.
---

### E44. Pre-run write-role guard (Snowflake extra)

> **Depends on E9.** Prevents the drift E12 repairs.

**What.** `macros/snowflake/assert_write_role.sql`, wired as an
`on-run-start` hook that is a no-op on non-Snowflake adapters. For
`run`/`build`/`seed`/`snapshot` against a shared environment database, it
fails unless `current_role()` is that database's service role. An allow-list
var names extra permitted roles.

**Why.** A developer running dbt against stage under a personal role creates
objects that role owns. The service role then cannot replace them, which is the
ownership drift E12 audits and repairs after the fact.

**How.** Map database → service role from the same names E9 uses. Skip
`compile`, `parse`, `docs`, `ls`, and personal (E31) databases.

**Caveat to document.** dbt runs `create schema if not exists` before
`on-run-start` fires, so a wrong role can still create an empty schema. The
guard stops table and view writes, not that.

**Acceptance.** On duckdb the hook runs and does nothing. Compiled on
Snowflake, a wrong role against a shared database aborts before the first
model; the service role and allow-listed roles pass; a personal database is
skipped.

---

## P8 — Dagster patterns, round 2

### E45. Restricted-tag reach invariant

> **Extends E5.**

**What.** `check_definitions(..., restricted={tag: allowed_jobs})` in
`utils/invariants.py`: fail if any job not in `allowed_jobs` has a resolved
asset selection that reaches an asset carrying `tag`.

**Why.** Broad selectors (`*`, a group, `upstream()`) pull in new assets as
they are added. When one of those reads data the job's role cannot access, the
job fails at run time in prod. Assets with restricted access need an explicit
list of jobs allowed to touch them.

**Acceptance.** A test definition where a broad-selection job reaches a tagged
asset fails the invariant, naming the job and asset. Adding the job to the
allow-list passes it.

---

### E46. Per-deployment schedule times

> **Extends E8's docs.**

**What.** A pure `schedules_for_deployment(name, base, overrides)` returning
`base` unchanged except in deployment `name`, where `overrides` replaces cron
strings by schedule name. A coverage test fails when a daily schedule is in
neither the override map nor an explicit `NOT_OVERRIDDEN` list.

**Why.** Stage and prod usually run the same schedules, but stage should run
off-peak or after prod's source loads. Copying the schedule list per
deployment drifts; overriding by name in one place does not. The coverage test
stops a new schedule from silently keeping prod's time in stage.

**Acceptance.** Unit tests: unknown deployment returns base; named deployment
gets the override; an override for a nonexistent schedule raises; a new daily
schedule missing from both lists fails the coverage test.

---

### E47. SQS sensor with redelivery-safe acknowledgement

**What.** `sensors/sqs_sensor.py` beside `s3_sensor.py`: polls a queue and
yields one `RunRequest` per message.

**How.**
- `run_key` is the message id, so a redelivered message produces no second run.
- Delete a message **one tick after** its run request was queued: the cursor
  stores message ids pending deletion, and the next tick deletes them. A crash
  between yielding the run and saving the cursor loses nothing (the message
  redelivers) and duplicates nothing (`run_key` dedups).
- Fail fast at load on an empty queue URL. `default_status=STOPPED`.

**Acceptance.** Tests with `moto`: a message produces one run; a simulated
crash before cursor save redelivers without a duplicate run; the message is
deleted on the following tick; an empty queue URL raises.

---

### E48. Warehouse backup as an asset

**What.** `build_backup_asset(table)` in `utils/factories.py`: an asset
downstream of a sync that copies `table` to Parquet in object storage.

**How.**
- Path: `<prefix>/<table>/snapshot_ts=<ts>/run_id=<run_id>/`. The prod/non-prod
  prefix comes from `utils/deployment.py`, with the same fail-safe: an
  unknown deployment writes under non-prod.
- Provenance (source relation, row count, snapshot timestamp, triggering run
  id, path) goes in asset materialization metadata.
- duckdb `COPY ... TO ... (FORMAT parquet)` by default.
- **Snowflake extra:** `COPY INTO @stage` in `macros/snowflake/` or a Snowflake
  resource method.

> **New mechanism:** production ran Snowflake `COPY INTO` only. The duckdb
> default is new.

**Acceptance.** A test materializes the asset against a local duckdb table and
a temp directory: the Parquet file exists at the expected path, row count in
metadata matches, and an unset deployment writes under the non-prod prefix.

---

### E49. Read-only Dagster status CLI

> **Depends on E14.**

**What.** `tooling/dagster-status/`: a stdlib CLI over the Dagster GraphQL API
(`/graphql` on OSS, and Dagster+ with a token). Commands: `status`, `recent`,
`errors`, `schedules`, `sensors`, `triggers <job>` (which schedules and
sensors can launch it, with their live state). Table or `--csv` output.
Queries only; no mutation is ever issued.

**Why.** "Is this job going to run tonight?" gets answered by reading code,
and code is wrong. `default_status` applies only on a definition's first load;
after that the live state is what counts, and a stopped schedule's job can
still be launched by a running sensor.

**How.** Add to `orchestration/dagster/CLAUDE.md`: query job state, never
infer it from `default_status` or schedule definitions.

> **New mechanism:** production used it against Dagster+ only. OSS `/graphql`
> support is new.

**Acceptance.** Tests mock GraphQL responses for each command and both output
formats. A test asserts no query string contains `mutation`. `triggers` lists a
running sensor for a job whose schedule is stopped.

---

## P9 — Pattern docs

> Docs under `docs/patterns/` unless noted. No new code unless the task says so.

### E50. Period close

`docs/patterns/period-close.md`:
- Read history **as of period end plus a grace window**, not as of the run
  date, so a retry next month reproduces the original numbers.
- Closed periods are locked unless explicitly listed in a reprocess var.
- The delete scope equals the reload scope; a mismatch deletes rows the run
  does not reload.
- A guard query runs before writing and is the same SQL as a singular test.
- As-of window and ordering logic live in one shared macro, so two filters
  cannot drift.
- Optional code: `assert_no_rows(sql, message)`, usable as a `pre_hook`.

**Acceptance.** If `assert_no_rows` ships, a test shows it aborting a model
build when the guard returns rows, on duckdb.

### E51. History tables stamped with `current_date`

Add to `docs/patterns/durable-facts.md`: a table stamping each row with
`current_date` must filter incrementally with `>=`, not `>`. With `>`, the
first run of each day writes that day and every later run skips it, so the day
freezes at its first state. Document a day-over-day shift test (a date whose
values equal the previous day's across all keys is suspect) and a repair op,
dry-run by default, that restates one bad date from a neighbouring day.

### E52. One unknown-member token per dimension

Add to E1's docs (`mint_surrogate_key` header and the dbt README): keep each
dimension's `null_as` token as its own named var (`unknown_customer`,
`unknown_product`). With one shared token, every dimension's unknown member
hashes to the same key, so a fact's unknown customer joins to the unknown
product.

### E53. Event-driven ingest → dbt subgraph → export

A section in `docs/architecture-patterns.md`:
- Ingest is its own job, separate from the dbt job. A table-materialized
  staging model run in the same job as ingest can read the table before the
  load finishes.
- Exports are side effects, so they run as jobs, not assets.
- Chain ingest → dbt subgraph → export with run-status sensors (E7) into the
  file-export engine (E23).

### E54. Image promotion runbook

`docs/patterns/image-promotion.md`: stage soak period, quiet windows, promote
by immutable tag (digest, never `latest`), rollback to the previous tag, image
retention long enough to roll back. This covers deployable images; branch
promotion paths stay declined (see *Deliberately left out*).

### E55. Sensitive-scan runbook: freeze, verify, audit

Add to `tooling/sensitive-scan/RUNBOOK.md`:
- **Not yet pushed:** amend or rebase locally; no history rewrite on the remote.
- **Freeze:** stop merges and pushes before rewriting.
- **Verify before force-pushing:** the rewritten history no longer contains the
  value (`git log -p --all | <scanner>`).
- **Scrub-completeness audit:** every branch, tag, and open PR ref checked
  after the force-push.
- **Branch protection:** lift it for the force-push only, restore it, and
  record both steps.

---

## P10 — Fixes to shipped code, round 2

### E56. Cost dates truncated in UTC

> **Fixes E27.**

**Why.** `int_cost_daily_allocation` derives `usage_date` with
`CAST(usage_hour AS DATE)` (two places). On Snowflake `usage_hour` is a
`TIMESTAMP_LTZ`, and the cast follows the session `TIMEZONE`. Hours near
midnight move to the neighbouring day, and on the last day of a month to the
neighbouring month. A laptop session and the prod service user then allocate
the same hours to different months, and the invoice tie drifts.

**How.**
- `utc_date_trunc(datepart, column)` in `macros/utils/`, through
  `adapter.dispatch`. Snowflake: `CONVERT_TIMEZONE('UTC', ...)::TIMESTAMP_NTZ`,
  then truncate. Default: `timezone('UTC', ...)`, then truncate. BigQuery:
  truncate as-is (`TIMESTAMP` is an instant, truncated in UTC). Returns naive
  UTC.
- Input must be zoned. SQL cannot see the type, and both Snowflake and duckdb
  read a naive input as session-local time, so the macro would shift an
  already-UTC value. Apply it once, in staging, where `start_time` enters as
  `TIMESTAMP_LTZ`: the two `dbt.date_trunc('hour', ...)` calls. Intermediate
  keeps `CAST(usage_hour AS DATE)`, which is zone-free on naive UTC.
- `usage_month` from ORGANIZATION_USAGE's `usage_date` stays on
  `dbt.date_trunc`: a `DATE` has no zone, and `CONVERT_TIMEZONE` would turn it
  into local midnight.
- The duckdb fixtures' `start_time` becomes `timestamptz` (values `+00`), so
  duckdb takes the same path as Snowflake.

**Acceptance.** On duckdb with `TimeZone = 'Pacific/Auckland'`, a
`timestamptz` hour at 23:00 UTC on 31 January maps to January. Compiled
Snowflake SQL contains the UTC conversion. Under `Pacific/Kiritimati` (UTC+14,
where the fixtures' 6 January hours fall on 7 January locally) the per-team
split matches UTC; it fails on the old staging code. The existing cost tests
and the invoice tie stay green.

---

### E57. Dagster: Airbyte assets from `AirbyteWorkspace`

> **Fixes shipped Dagster code** (`src/utils/factories.py`,
> `src/assets/airbyte.py`).

**Why.** Both files build assets with `build_airbyte_assets` over
`AirbyteResource`, the pre-workspace API. dagster-airbyte's migration guide for
Airbyte 1.6+ (which `abctl` installs) moves to `AirbyteWorkspace` and
`build_airbyte_assets_definitions`. The workspace API reads connections and
streams from Airbyte, so table lists stop being hand-maintained.

**How.**
- An `AirbyteWorkspace` resource from `EnvVar`s replaces `AirbyteResource`.
- `build_airbyte_assets_definitions(workspace=...)` with a
  `connection_selector_fn` replaces the per-connection factory calls.
- A translator keeps today's asset keys, so dbt sources and downstream deps do
  not move.
- `build_backup_asset` stays in `factories.py`; `build_source_assets` goes if
  nothing else calls it.

**Acceptance.** `just dagster::test` passes with the workspace API mocked. A
test lists asset keys and matches the pre-migration list. No import of
`build_airbyte_assets` or `AirbyteResource` remains.

---

## P11 — Cheap guards

### E58. Fail on ignored `check` constraints (Snowflake extra)

**Why.** dbt-snowflake ignores `type: check` on an enforced contract, so the
contract reads as enforced and the check never reaches the table.
`type: custom` with `expression: "check (...)"` renders the clause.

**How.** `run-operation assert_no_check_constraints` in `macros/snowflake/`. It
reads `graph.nodes` only, so it runs on any adapter, and fails listing every
`model.column` with a `type: check` constraint. Wire it into `just dbt::test`
and CI. Document the `custom` form in `transformation/dbt/CLAUDE.md`.

**Acceptance.** A fixture model with `type: check` fails the operation, naming
the model and column. `type: custom` passes. The example project passes.

---

### E59. Definition invariant: scheduled dbt jobs use `build`

> **Extends E5.**

**Why.** `dbt run` then `dbt test` materialises a model's children before its
tests fail. With `build`, a failed test skips its children, and they keep their
last good data.

**How.** Add to the E5 suite: every scheduled or sensor-triggered job that
invokes dbt calls `build`, never `run`. Same rule in the Airflow and Prefect
tests where they invoke dbt.

**Acceptance.** A test definition calling `run` fails the invariant. The
shipped definitions pass.

---

## P12 — Source contracts, keys and history

> dbt building blocks, round 3. All run on duckdb unless marked.

### E60. Source capability classification

**What.** `docs/patterns/source-types.md` and a seed template that declares
each source's type.

| Type | Delivery | Clock |
|---|---|---|
| CDC | changes, with operation and log position | log position |
| Append-only | new rows only; rows never change | create time |
| Full-replace | full state each delivery | none needed |
| Mutable with a clock | changed rows | moves on update |
| Mutable without a clock | full state or changed rows | create only, or none |

Modifier on any type: **can signal deletes**.

Three questions decide the type:
1. Does each delivery carry full state, or only changes?
2. Does the clock move on update, or only on create?
3. Can you tell a delete from an omission?

**Why.** Incremental strategy, dedup, delete handling and history all depend on
what the source can tell you. Chosen per model without naming the type, they
give incremental models that miss updates (filtering on a create-only clock) or
never see deletes.

**How.**
- One permits/forbids table per type. Example: mutable without a clock forbids
  an incremental filter on its clock; full-replace permits an anti-join soft
  delete.
- Seed template columns: `source_name`, `table_name`, `source_type`,
  `signals_deletes`, `clock_column`, `clock_moves_on_update`, `owner`.

**Acceptance.** The doc runs three invented feeds (`orders` changes,
`customers` full export, an event log) through the three questions to one type
each. The seed template loads in `dbt build` on the example fixtures.

---

### E61. Incremental patterns per source type

> **Depends on E60.**

**What.** `docs/patterns/incremental.md`, one example model per source type,
and a generic test `no_tie_at_max_clock(clock, partition_by=[],
tie_breakers=[], scope='max')`.

**How.**
- Standard CTE flow: `source → typed → hashed → deduped → changed → final`.
- `merge` with no `unique_key` is an append. The doc says so; E68 reports it.
- `no_tie_at_max_clock` fails when two rows in a partition share the ordering
  tuple `(clock, *tie_breakers)`. A tie means "latest" or "next after the
  cursor" is arbitrary. One test covers each tie-breaking moment through its
  arguments:

  | Moment | `partition_by` | `tie_breakers` (example) | `scope` |
  |---|---|---|---|
  | Dedup to the latest version of a key | entity key | ingestion id | `max` |
  | Incremental high-water mark (`>` skips late rows, `>=` re-reads them) | none | raw id | `max` |
  | Ordering every version for history (E65) | entity key | ingestion id | `all` |
  | CDC snapshot chunks sharing one LSN (E67) | entity key | `_airbyte_extracted_at`, `_airbyte_raw_id` | `all` |

  `scope='max'` checks only the top tuple per partition; `'all'` checks every
  rank. The doc pairs each source type (E60) with its tie-breaker columns.
- Full-replace feeds: replace by load partition, or an anti-join soft delete
  behind a partial-delivery guard. The guard refuses to soft-delete when a
  delivery's row count is below a set share of the previous delivery.
- Caveats: microbatch is delete+insert per batch underneath, and an upstream
  ref with no `event_time` is scanned in full for every batch.

**Acceptance.** Each example model builds twice on duckdb (initial fixture,
then a second batch) and produces the expected rows. The tie test fails on a
fixture for each moment in the table and passes once its tie-breakers are
added. The guard blocks the soft delete when the second delivery
is truncated.

---

### E62. Key roles and the identity, content and link hashes

> **Extends E1 and E40.**

**What.** `docs/patterns/keys.md`, plus an audit-column check in
`row_content_hash`.

| Key role | Meaning |
|---|---|
| Entity key | stable across versions; identity hash (E1) over the natural key |
| Version key | one per entity per validity interval |
| Natural-key attribute | the source's own id, kept as an attribute |

| Hash | Over | Used for |
|---|---|---|
| Identity | natural key | entity key (`mint_surrogate_key`) |
| Content | non-key, non-audit columns | change detection (`row_content_hash`) |
| Link | participating entity keys, in declared order | relationship rows (`mint_surrogate_key`; no new macro) |

**Why.** A join on an entity key with no validity predicate fans out against a
history table. A join on a natural key after staging breaks when the source
reissues or reformats it.

**How.**
- Join rule: entity key plus a validity predicate, or the version key. Natural
  keys are never join keys after staging.
- `row_content_hash` fails at compile time when its column set includes an
  audit column (the `audit_columns` macro's names) that is not excluded. It
  already sorts columns and carries `row_content_hash_version()`.

**Acceptance.** `row_content_hash` over a relation with an unexcluded audit
column fails to compile. The E40 tests still pass. A singular test reproduces
the fan-out: it fails on the join without the validity predicate and passes
with it.

---

### E63. Exclusion signals

> **Extends E4.** E4 logs rule failures beside the data; this puts the
> decision on the row.

**What.** Flag bad rows instead of dropping them. A `dq_exclusion_code` column
(first matching rule wins; NULL means usable) is carried through every model.

**Why.** A filter in staging removes a row from every consumer and leaves no
record. One consumer wants it gone, another wants to review it, a third wants
it counted.

**How.**
- `dq_exclusion_code(rules)`: a `case` over ordered rules, read from a
  reason-code seed.
- Reason-code seed: `code`, `description`, `kind` (`mandated` | `internal`).
  Mandated rules cite their `authority`; internal rules carry `owner`,
  `rationale` and `review_by`. A test enforces both.
- Each consumer chooses: filter on `dq_exclusion_code is null`, keep flagged
  rows for review, or aggregate by code.
- `dq_excluded_rows` unions the excluded rows of every flagged model (model,
  key, code).
- Conservation test per flagged model: rows in = rows excluded + rows out.

**Acceptance.** A fixture row matching two rules gets the first rule's code.
The conservation test fails when a model silently drops a flagged row. The seed
test fails on a mandated rule with no authority, and on an internal rule past
`review_by`.

---

### E64. Test ladder

> **Updates E18** (`dbt-test` skill).

Add to `.claude/skills/dbt-test`:
- **Sources:** content tests (`not_null`, `accepted_values`, row-count bounds),
  not only freshness.
- **Passthrough staging** (rename and cast only): no tests of its own.
- **Every intermediate hop:** a grain test (`unique`,
  `unique_combination_of_columns`, or E41's `grain_guard_is_one`).
- **Joins:** `relationships` tests on the join keys.
- **Marts:** re-assert grain and keys.
- **Diagnosis:** a red mart over green upstream means the bug is in assembly,
  the mart's own join or aggregation.
- Generic test `config_rows_present(config_seed, union_model, key)`: every row
  marked active in a config seed shows up in its union model.

**Acceptance.** `config_rows_present` fails on a fixture where an active config
row has no rows in the union model. The example cost models follow the ladder.

---

### E65. Managed history from an observation log

> **Extends E32.** Depends on E62.

**What.** An append-only observation log keyed on `(entity_key, observed_at)`,
a history table derived from it, an `as_of(rel, date)` macro, and a short guide
to picking an SCD type.

**Why.** History built by grouping on the content hash merges the two A periods
when a value goes A → B → A, losing the second change. A log keeps every
observation, so history can be re-derived; a snapshot table cannot be rebuilt.

**How.**
- Log: one row per entity per observation, with the E40 content hash.
  `full_refresh: false`.
- History: islands of consecutive equal hashes per entity (as in E38), giving
  `valid_from`, `valid_to`, `is_current`.
- `as_of(rel, date)`: half-open, `valid_from <= date and date < valid_to`. Open
  versions use a sentinel `valid_to` (`9999-12-31`), so the predicate has no
  NULL branch.
- Observed time (when we saw it) and effective time (when the source says it
  took effect) are separate columns; `as_of` takes which one to use.
- SCD guide in `docs/patterns/snapshots.md`: type 1, type 2 snapshot,
  log-derived, and when each fits.

**Acceptance.** A fixture A → B → A yields three versions. `as_of` returns
exactly one row per entity for any date, including a boundary date. E39 passes
on the derived history.

---

### E66. Snapshot migration hazards

Add to `docs/patterns/snapshots.md`, with a row each in the
`docs/patterns/state-migrations.md` table:
- **Switching to `check_cols=['content_hash']`:** backfill the hash on existing
  snapshot rows first, or every row opens a new version on the next run.
- **Changing `unique_key`:** existing history is orphaned; its open versions
  never close. Rewrite the key on existing rows before switching.
- **`dbt_scd_id`** hashes the run timestamp, so a rebuilt snapshot gets new
  values. Nothing downstream joins on it.

---

### E67. CDC landing tables: current state and history

> **Reverses a *Deliberately left out* entry.** An Airbyte pattern; E38 stays
> the connector-free route.

**What.** `docs/patterns/cdc-landing.md` and `cdc_current(rel, key)`, with
Airbyte's CDC and meta column names as overridable defaults.

**How.**
- Order events by LSN, then `_airbyte_extracted_at`, then `_airbyte_raw_id`:
  initial-snapshot chunks share one LSN.
- Tombstones drop out of current state and stay in history as deletes.
- A stream refresh purges the landing table's history, so the dbt history model
  is the system of record: incremental, `full_refresh: false`.
- Collapse no-op events (same payload hash as the key's previous event).
- Alert when `_airbyte_generation_id` changes.
- Sync mode: dedup drops deletes; append keeps them. Use append when deletes
  matter.

**Acceptance.** A fixture landing table with snapshot chunks sharing an LSN, an
update, a no-op, a delete and a generation change: `cdc_current` returns the
expected rows, history keeps the delete and drops the no-op, and the
generation-change test fires.

---

## P13 — Project-wide tooling

### E68. dbt baseline census

> **New tool:** `tooling/dbt-baseline`.

**What.** A CLI over `manifest.json` from `dbt parse` (no warehouse
credentials) that reports a project's current shape, as Markdown and JSON:
- materialization mix, per folder
- `merge` models with no `unique_key`
- snapshots using `check_cols='all'` or a concatenated `unique_key`
- which models have grain and `relationships` tests (the E64 ladder)
- enforced contracts
- multi-column joins, parsed from compiled SQL when the manifest has it;
  otherwise reported as skipped
- source `loaded_at_field`s that the E60 seed marks as moving only on create

Optional flag: fail on joins on natural-key attributes after staging, using
E62's key roles from model `meta`.

**Acceptance.** A fixture manifest covering every finding produces the expected
report. It runs on the toolkit's own project with no warehouse credentials. The
tool passes `just check-wiring`.

---

### E69. PR impact checker

> **New tool:** `tooling/pr-impact`. Builds on E24.

**What.** `dbt parse` on base and head (no warehouse credentials), a manifest
diff, a severity per change by whether it reaches an exposure, and one sticky
PR comment.

| Change | Severity |
|---|---|
| Exported column removed or renamed (columns from the E23 export config) | block (failing check) |
| Change upstream of an exposure | review |
| Anything else | info |

**How.**
- Both parses use the same vars. The base parse is cached as an artifact keyed
  on the base SHA.
- Triggered on `pull_request`, never `pull_request_target`: `dbt parse` runs
  Jinja from the PR. Fork PRs get the result in the job summary, since their
  token cannot comment.
- One comment, updated in place.

**Acceptance.** Fixture base/head project pairs produce block, review and info.
The workflow uses `pull_request`. A test with a fake GitHub client shows the
comment updated, not duplicated.

---

### E70. Consumer blast-radius audit (Snowflake extra)

> **Promotes** *Pre-decommission consumer audit* from the deferred list.

**What.** For objects being changed or retired, who reads them and what moving
each reader costs. `macros/snowflake/` plus a report model.

**How.**
- Consumers from `ACCESS_HISTORY` (direct and base objects): users, roles,
  query tags.
- Walk `OBJECT_DEPENDENCIES` to every dependant, with a visited set as the
  cycle guard.
- Drop non-prod objects (E9 and E31 names).
- Tier consumers by migration cost. An `unknown` tier blocks.
- Status is `observed` with a last-seen date, never a boolean `ready`: absence
  in the window is not proof.
- Windows: 365 days to discover consumers, 30 days to track progress.
- Add to `docs/patterns/durable-facts.md`: an audit fact can be durable because
  its inputs are read from current state.

**Acceptance.** On duckdb, fixture tables shaped like `ACCESS_HISTORY` and
`OBJECT_DEPENDENCIES` (with a cycle) give the expected consumers and tiers, and
one `unknown` consumer marks the report blocked. Compiles on Snowflake.

---

## P14 — Environments, identity and runbooks

### E71. Clone hygiene (Snowflake extra)

> **Extends E10 and E31.** `macros/snowflake/`.

- `drop_database(name)`, guarded: refuses prod and shared environment
  databases; allowed for personal (E31) and clone names only.
- Idle-clone sweep: lists clones unused for N days, drops them only with an
  apply flag.
- `DATA_RETENTION_TIME_IN_DAYS = 0` on clones that a refresh replaces, so
  replaced clones hold no Time Travel storage.
- `clone_schema`, alongside `clone_database`.
- Refresh only after the upstream run succeeds (E7), not on a clock.

**Acceptance.** The drop guard refuses prod and shared names and allows a clone
name. The sweep's dry run lists only clones. `clone_schema` and the retention
setting compile on Snowflake.

---

### E72. Password to key-pair cutover runbook

`docs/patterns/key-pair-cutover.md`:
- Issue a new key-only identity; do not convert the password user in place.
- Read the username from config, so falling back is a config change.
- Always pass the role explicitly.
- Rotate with the two public-key slots: load the new key, switch, then clear
  the old slot.
- Look for connection paths inside packaged dependencies, not only the
  project's own config.
- Retire the old identity only after a full monthly cycle with zero password
  logins in login history.

---

### E73. Postgres replication-slot guard

> Dagster reference implementation.

**Why.** A logical replication slot whose consumer stalls keeps WAL until the
disk fills.

**How.**
- Measure slot lag (`pg_replication_slots`, `restart_lsn` against the current
  WAL position) and free disk (a host or provider metric).
- Three tiers on either signal: warn, page (E6 severities), then auto-drop.
- Auto-drop targets one slot named in config, no pattern matching. Dry run by
  default; idempotent, so a missing slot is a no-op.
- Recovery after a drop is a full resync of the connector; the runbook says how.

**Acceptance.** With a fake connection, each tier fires at its threshold. The
drop runs only on the configured slot, only with dry run off, and a second run
succeeds.

---

### E74. `validate-on-clone` stack skill

`.claude/skills/validate-on-clone`: check a change against a clone before
merge.
- Base manifest from `dbt parse` with the same vars as prod, cached as an
  artifact.
- `run`, then `test --no-fail-fast`, then a final `build` shaped like the prod
  job.
- Check selectors with `dbt ls` first: `state:modified+,tag:x` is an
  intersection.
- `on_schema_change` conflicts show only on a populated clone, not an empty
  one.

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

Reversed: *history reconstruction from CDC landing tables* was listed here and
is now E67.

---

## Deferred to a later round

Backlog notes, not tasks. They get E-numbers when promoted.

- **LLM enrichment inside a dbt DAG.** A Python asset between models; results
  keyed on id + input hash + per-stage prompt version in an append-only table;
  an anti-join so only new rows are billed; an attempt cap so unparseable
  responses stop re-billing; a per-run budget; a "latest row per stage" dbt
  model; a reconciliation test as the gate. Deferred: the production version
  is not proven yet.
- **Engineering-analytics example stack** over issue-tracker and code-host
  data: an identity bridge between the two systems' users, issue flow time, PR
  time-to-first-review, throughput.
- **Per-unit cost metrics gated on coverage.** NULL before coverage starts;
  each period gated on a share of the median entity count; a test on the
  median itself.
- **Cost attribution for platform-run work** (extends E27). Attribute tasks and
  dynamic tables by the database they write to, with a name-pattern fallback
  second in the lookup order. An `is_attribution_complete` flag, since gaps
  read as fake idle. Rank right-sizing by idle dollars, not idle %.
- **Load-timestamp sensor.** Run downstream only when a source's load
  timestamp moved; keep one reconciliation run on cron.
- **Per-query telemetry.** `query_tag = invocation_id|model_id`, then pull
  spill and queue time per model. Per-folder `+query_tag` in
  `dbt_project.yml` for cost by domain.
- **Source cutover via passthrough shims** plus a parity check. Mostly covered
  by E34.
- **Referential-integrity rollout.** FK tests land at `warn`, each orphan set
  gets an owner, then the tests go to `error`.
- **History-scrub runbook** (extends E55): fetch `refs/pull/*`, list PR refs
  that still hold a removed path, verify fresh clones.
- **`NOT IN` trap in file ingestion.** One NULL in the subquery stops ingestion
  forever. Use `NOT EXISTS`, and keep a ledger of filename plus size or etag.
- **Durable-facts caveats** (E29): a mapping-seed change only restates the
  restatement window; an invoice tie cannot detect lost source history.
- **Source material not yet reviewed for ports:** its delivery, AI-bot,
  intake-template and review-practice pages.
