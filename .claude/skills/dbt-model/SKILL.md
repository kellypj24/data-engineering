---
name: dbt-model
description: Use when adding or scaffolding a dbt model — "add a staging model for orders", "create an intermediate model", "build a mart", "new dbt model", "stg_/int_/fct_ model". Writes the .sql and its paired .yml, honouring the project's macros, layer materialisations, and sqlfluff rules.
argument-hint: "<layer> <model-name>   e.g. staging stg_customers"
---

# Add a dbt model

This skill runs **in whatever project the dbt tool was copied into**, not in the
toolkit. Never assume a path like `transformation/dbt/`.

## Step 0 — locate the project and read its conventions

```bash
find . -name dbt_project.yml -not -path "*/dbt_packages/*" -not -path "*/.venv/*"
```

That file's directory is the **dbt root**; every command below runs from there.
Read it before writing anything — it is the source of truth for two things:

- **Which layers exist**, and what each is materialised as. In the toolkit's
  project that is `staging` → view, `intermediate` → view, `marts` → table, each
  with a `+schema`. A downstream project may have renamed or added layers.
- **Whether your layer is already configured.** Set materialisation in a model's
  `config()` block only to *deviate* from the layer default. Restating the
  default is noise that drifts.

Then read one existing model in the target layer. It outranks anything in this
file — match the project you are in. If the layer is empty, `models/staging/`
in the toolkit ships `stg_example.sql` / `stg_example.yml` as the reference
shape.

## Step 1 — name it

- `staging/` — `stg_<source>_<entity>`, one model per source table, 1:1.
- `intermediate/` — `int_<entity>_<verb>`, business logic and joins.
- `marts/` — consumer-facing. Check sibling models for the project's convention
  before defaulting to the usual dbt `fct_` / `dim_` split.

## Step 2 — write the `.sql`

Staging models follow a CTE shape — source, then rename/cast:

```sql
WITH source AS (

    SELECT *
    FROM {{ source('raw', 'orders') }}
    WHERE {{ limit_data_in_dev('created_at') }}

),

renamed AS (

    SELECT
        id AS order_id,
        {{ clean_string('status') }} AS status,
        amount,
        created_at,
        {{ audit_columns() }}
    FROM source

)

SELECT * FROM renamed
;
```

The project's macros, and the constraints each one puts on you:

| Macro | Signature | Constraint |
|---|---|---|
| `audit_columns` | `(loaded_at_column=none)` | Emits **two** columns and **no trailing comma** — it must be the **last** entry in the `SELECT` list. Pass the EL timestamp (e.g. `'_airbyte_extracted_at'`) to preserve the real load time; bare `()` falls back to `CURRENT_TIMESTAMP()`. |
| `clean_string` | `(column_name)` | `TRIM` + `LOWER` + empty-to-`NULL`. Returns an expression, so it still needs your `AS <name>`. |
| `limit_data_in_dev` | `(column_name, dev_days_of_data=3)` | A **complete predicate**: the recency filter outside prod, `TRUE` in prod. Use it as the whole `WHERE`, or compose with `AND`. No `WHERE 1 = 1` anchor needed. |
| `safe_divide` | `(numerator, denominator)` | Null/zero-safe division. Use it instead of `/` in marts. |

Schema routing is handled by the `generate_schema_name` override — non-prod
prefixes with the target's schema (`main_staging`), prod uses the bare schema
(`staging`). **Never hardcode a schema**; use `+schema` in `dbt_project.yml` or
the model's `config()`.

Environment comes from the **`dbt_env` var** (`DBT_ENV`, default `dev`), never
from `target.name` — the target picks a warehouse, not an environment. If you
write a macro that branches per environment, branch on `var('dbt_env')`.

### sqlfluff rules that actually bite

The `.sqlfluff` in the dbt root is authoritative. The ones that reject a model
most often:

- Keywords, functions, literals, and types **UPPER**; identifiers **lower**.
- **Trailing commas in `SELECT` are forbidden.**
- **No final semicolon.** dbt wraps the model in `create ... as (...)`, so a
  terminator is a syntax error on every adapter.
- Aliasing is **explicit** — `AS` is mandatory, and aliases are **≥2 characters**.
- CTEs are not indented; 4-space indent; 120-char lines.

## Step 3 — write the paired `.yml`

**One `.yml` per model, named after the model** — `stg_orders.sql` pairs with
`stg_orders.yml`. Do not create or append to a shared `_schema.yml`; this
project deliberately keeps them one-to-one so a model and its contract move
together.

Every model needs a **description** and **at least one test**, because
`.pre-commit-config.yaml` runs dbt-checkpoint's `check-model-has-description`
and `check-model-has-tests --test-cnt 1`. A model without them is rejected
before review.

```yaml
version: 2

models:
  - name: stg_orders
    description: Staged orders with cleaned strings and audit columns
    columns:
      - name: order_id
        description: Primary key
        tests:
          - unique
          - not_null
```

Describe the audit columns too — `_loaded_at` and `_dbt_updated_at` are columns
like any other, and a bare column with no description is a review comment
waiting to happen.

If the logic is non-trivial (string cleaning, conditional joins, a
`safe_divide`), add a `unit_tests:` block to the same file. `stg_example.yml`
shows the shape: `given:` with `input: source(...)` rows, `expect:` with the
result rows. Unit tests need `require-dbt-version: ">=1.8.0"`, which this
project sets.

## Step 4 — sources must exist first

`{{ source('raw', 'orders') }}` resolves only if that table is declared in
`models/staging/_sources.yml`. If it isn't, add it there (with a description and
tests on its key) before the model will parse — or use the `dbt-source` skill,
which does both halves.

## Verify

From the dbt root:

```bash
uv run dbt deps          # first run only; sqlfluff's dbt templater needs the packages
uv run dbt parse         # the model and its yml are valid and resolve
uv run dbt compile --select <model_name>
uv run sqlfluff lint models/<layer>/<model_name>.sql
```

If the project has a reachable warehouse and real source data, finish with:

```bash
uv run dbt build --select <model_name>
```

`dbt build` is the real proof, but it needs the sources to exist. In the
**toolkit's own** copy they do not — there is no `raw` schema, so `dbt test` and
`dbt build` fail by design (tracked as task #16 in `docs/ci-cd-hardening.md`).
There, `parse` + `compile` + `lint` is the complete gauntlet. Downstream, it is
not — do not stop early and call a model verified.

`profiles.yml` lives in the project directory here, not `~/.dbt`, so
`DBT_PROFILES_DIR` must point at the dbt root. The `mod.just` exports it; if you
are calling `dbt` directly, export it yourself.

## Common mistakes

- **Putting `{{ audit_columns() }}` mid-`SELECT`.** It emits two columns with no
  trailing comma, so anything after it is a syntax error. It goes last.
- **Writing `WHERE 1 = 1`.** Nothing in this project needs it any more —
  `limit_data_in_dev` returns a complete predicate. A bare `WHERE 1 = 1` reads
  as though a filter was deleted. (Incremental models are the one place a
  no-op anchor can earn its keep, to keep an `{% if is_incremental() %}` block
  readable.)
- **Branching a macro on `target.name == 'dev'` / `'prod'`.** The targets are
  named for warehouses (`duckdb`, `snowflake`), so those comparisons are never
  true and the branch silently vanishes. Both project macros shipped with this
  bug. Use `var('dbt_env')`.
- **Aliasing a column to a dialect keyword.** `AS email` fails sqlfluff `RF04`
  under the Snowflake dialect even though the source column is named `email`.
  Confirmed while writing this skill. `email_address` passes; check any alias
  that is a bare common noun.
- **Creating `_schema.yml`.** One `.yml` per `.sql`, named to match.
- **Shipping a model with no description or no test.** dbt-checkpoint blocks it.
  This is the single most common reason a model bounces.
- **Restating the layer's materialisation** in `config()` when it already
  matches `dbt_project.yml`. Two sources of truth, one of which will go stale.
- **Trailing comma before `FROM`** is a sqlfluff failure, invisible until you
  lint. A **final semicolon** lints clean but fails at `dbt build` with
  `syntax error at or near ";"`.
- **Running `sqlfluff` before `dbt deps`.** The templater compiles the project to
  lint it, so a missing package surfaces as a confusing lint error rather than a
  missing-dependency one.
- **Hardcoding `dev_staging` or any other schema.** That is
  `generate_schema_name`'s job; hardcoding breaks the prod path silently.
