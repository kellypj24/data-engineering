---
name: data-profiling
description: Use before writing a staging model or tests for a raw table — "profile this table", "what does the raw X data look like", "check nulls/whitespace/casing in source Y", "what tests should this source have". Profiles every column with generic SQL through `dbt show`, then turns the findings into source tests and staging cleaning.
argument-hint: "<source-name> <table-name>   e.g. raw orders"
---

# Profile a raw table

Runs **in the downstream project**. Locate the dbt root first:

```bash
find . -name dbt_project.yml -not -path "*/dbt_packages/*" -not -path "*/.venv/*"
```

All commands run from there with `DBT_PROFILES_DIR` pointing at it (the
project's `mod.just` exports it).

Profile before writing anything. Tests and cleaning written from assumptions
about a table encode the assumptions, not the data. The profile is the evidence
the **`dbt-source`** / **`dbt-test`** (tests) and **`dbt-model`** (cleaning) skills act on.

Every query goes through `dbt show --inline`, so `{{ source() }}` resolves and
nothing is created in the warehouse. Pass the row limit with `--limit`. A
`LIMIT` inside the SQL collides with the one `dbt show` adds and fails to parse.

## Step 1 — size it, and sample if large

```bash
uv run dbt show --inline "SELECT COUNT(*) AS n FROM {{ source('raw', 'orders') }}"
```

Above ~1M rows, profile a sample: replace `FROM {{ relation }}` in the queries
below with the adapter's sampling clause. Sampling is not portable:

| Adapter | Sample ~10% |
|---------|-------------|
| duckdb | `FROM {{ relation }} USING SAMPLE 10%` |
| Snowflake | `FROM {{ relation }} SAMPLE (10)` |
| Postgres | `FROM {{ relation }} TABLESAMPLE SYSTEM (10)` |
| BigQuery | `FROM {{ relation }} TABLESAMPLE SYSTEM (10 PERCENT)` |

Counts from a sample are estimates. Say so when you report them. Distinct
counts in particular do not scale with the sample.

## Step 2 — one row per column

Save as `profile.sql` (outside `models/`), set the source, and run
`uv run dbt show --limit 500 --inline "$(cat profile.sql)"`. Add
`--output json` for every column untruncated.

```sql
{%- set relation = source('raw', 'orders') -%}
{%- for col in adapter.get_columns_in_relation(relation) %}
SELECT
    '{{ col.name }}' AS column_name,
    '{{ col.data_type }}' AS data_type,
    COUNT(*) AS row_count,
    COUNT(*) - COUNT({{ col.quoted }}) AS nulls,
    COUNT(DISTINCT {{ col.quoted }}) AS distinct_values,
    {%- if col.is_string() %}
    SUM(CASE WHEN TRIM({{ col.quoted }}) = '' THEN 1 ELSE 0 END) AS empty_strings,
    SUM(CASE WHEN {{ col.quoted }} <> TRIM({{ col.quoted }}) THEN 1 ELSE 0 END) AS untrimmed,
    SUM(CASE WHEN {{ col.quoted }} <> LOWER({{ col.quoted }}) THEN 1 ELSE 0 END) AS not_lowercase,
    COUNT(DISTINCT {{ col.quoted }}) - COUNT(DISTINCT LOWER(TRIM({{ col.quoted }}))) AS case_or_space_variants,
    SUM(CASE WHEN TRIM({{ col.quoted }}) <> ''
              AND TRANSLATE(TRIM({{ col.quoted }}), '0123456789.-', '') = '' THEN 1 ELSE 0 END) AS numeric_looking,
    {%- else %}
    NULL AS empty_strings, NULL AS untrimmed, NULL AS not_lowercase,
    NULL AS case_or_space_variants, NULL AS numeric_looking,
    {%- endif %}
    CAST(MIN({{ col.quoted }}) AS {{ dbt.type_string() }}) AS min_value,
    CAST(MAX({{ col.quoted }}) AS {{ dbt.type_string() }}) AS max_value
FROM {{ relation }}
{% if not loop.last %}UNION ALL{% endif %}
{%- endfor %}
```

It is generic SQL: `TRIM`, `LOWER`, `TRANSLATE`, `COUNT(DISTINCT)` exist on
duckdb, Snowflake, Postgres, and BigQuery. `numeric_looking` uses `TRANSLATE`
rather than a safe cast on purpose: `dbt.safe_cast` is a plain `CAST` on
duckdb and Postgres, and errors on the first non-number.

## Step 3 — distributions and outliers, where step 2 points

Low-cardinality columns (`distinct_values` small): the value distribution.

```bash
uv run dbt show --limit 20 --inline "SELECT status AS value, COUNT(*) AS n
  FROM {{ source('raw', 'orders') }} GROUP BY 1 ORDER BY 2 DESC"
```

Numeric columns: the spread.

```bash
uv run dbt show --inline "SELECT MIN(amount) AS min_value,
  PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY amount) AS p01,
  PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY amount) AS p50,
  PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY amount) AS p99,
  MAX(amount) AS max_value FROM {{ source('raw', 'orders') }}"
```

`PERCENTILE_CONT ... WITHIN GROUP` works on duckdb, Snowflake, and Postgres.
On BigQuery use `APPROX_QUANTILES(amount, 100)[OFFSET(1)]` and friends.

## Step 4 — turn findings into tests and cleaning

| Finding | Test (`dbt-source` / `dbt-test`) | Cleaning (`dbt-model`) |
|---------|----------------------------------|------------------------|
| key with `nulls = 0`, `distinct_values = row_count` | `unique` + `not_null` on the source key | — |
| `nulls` > 0 on a column that must be present | `not_null` only if the business says so; otherwise document it | `COALESCE` only with an agreed default |
| `empty_strings` > 0 | — | `clean_string` (NULLIF of the trimmed value) |
| `untrimmed` / `case_or_space_variants` > 0 | `accepted_values` on the **cleaned** staging column | `clean_string` (TRIM + LOWER) |
| small, stable value set | `accepted_values` on the staging column | — |
| `distinct_values` = 1 | none: flag it. A constant column is often a broken extract | consider dropping it |
| `numeric_looking` < non-empty count on a string column | — | mixed types: `TRY_CAST`/`SAFE_CAST` per adapter, and decide what a non-number means |
| p99 far from max, or negatives where none make sense | a bounded `dbt_utils.accepted_range` only if the bound is a business rule | — |

Put tests on the **staging** column after cleaning wherever possible. A source
test runs against data you do not control and fails for reasons nobody can
fix.

## Verify

```bash
uv run dbt show --limit 500 --inline "$(cat profile.sql)"   # exits 0 and prints one row per column
```

Then report the profile, with the sample caveat if sampled, before writing
tests or SQL.

## Common mistakes

- **`LIMIT` inside `dbt show --inline`.** `dbt show` wraps your SQL in its own
  limit; a second one is a parse error. Use `--limit`.
- **`dbt.safe_cast` to find non-numbers.** It is a plain `CAST` on duckdb and
  Postgres and errors on the first bad value. Use the `TRANSLATE` check.
- **Profiling a sample and reporting exact counts.** Distinct counts do not
  scale; say the numbers are from a sample.
- **Writing `accepted_values` against the raw column.** The untrimmed,
  mixed-case variants fail it forever. Clean in staging, then test there.
- **Treating a constant column as fine.** One distinct value across millions of
  rows is usually an extract bug, not a feature.
