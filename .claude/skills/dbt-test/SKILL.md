---
name: dbt-test
description: Use when writing or reviewing a dbt model's tests and docs — "add tests to model X", "what's the grain of this model", "write the yml for fct_y", "this model has duplicates", "unique test passes but the data is wrong". Establishes the grain first, then writes the uniqueness test, doc-block descriptions, and only the tests the data supports.
argument-hint: "<model-name>   e.g. fct_monthly_cost"
---

# Test a dbt model

Runs **in the downstream project**. Locate the dbt root first:

```bash
find . -name dbt_project.yml -not -path "*/dbt_packages/*" -not -path "*/.venv/*"
```

The `.yml` sits beside the model, one per model (see the project's
`CLAUDE.md`). This skill writes or updates it.

## Step 1 — the grain, before anything else

Write down, in one sentence, what one row is: "one row per month, team,
workload, warehouse, and currency." Every other test depends on getting this
right, and it is the one most often wrong.

Prove it against the data. The grain query must return no rows:

```bash
uv run dbt show --inline "SELECT cost_month, team, workload, warehouse_name, currency, COUNT(*) AS n
  FROM {{ ref('fct_monthly_cost') }}
  GROUP BY 1, 2, 3, 4, 5 HAVING COUNT(*) > 1"
```

Then prove it is **minimal**: drop each grain column in turn and confirm
duplicates appear. A column whose removal changes nothing is not part of the
grain. A superset of the real grain makes the uniqueness test pass while the
model fans out.

```bash
uv run dbt show --inline "SELECT COUNT(*) AS rows_,
  COUNT(DISTINCT cost_month || '|' || team || '|' || workload || '|' || warehouse_name || '|' || currency) AS full_grain,
  COUNT(DISTINCT cost_month || '|' || team || '|' || workload || '|' || currency) AS without_warehouse
  FROM {{ ref('fct_monthly_cost') }}"
```

`rows_ = full_grain` proves uniqueness. `without_warehouse < full_grain` proves
`warehouse_name` belongs in it. Cast non-string columns before concatenating
on adapters that do not do it implicitly.

## Step 2 — the uniqueness test that matches the grain

| Grain | Test |
|-------|------|
| one column | `unique` + `not_null` on that column |
| several columns | `dbt_utils.unique_combination_of_columns` at model level, plus `not_null` on each column |

```yaml
models:
  - name: fct_monthly_cost
    description: >
      ... Grain: one row per cost_month, team, workload, warehouse_name, currency.
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - cost_month
            - team
            - workload
            - warehouse_name
            - currency
```

State the grain in the model description too. The test enforces it, and the
description is where a reader finds it.

## Step 3 — descriptions, doc blocks first

A column that means the same thing in several models (`cost_month`, `team`)
gets **one** doc block, referenced everywhere. Copies drift.

```markdown
{# models/cost/_cost_docs.md #}
{% docs cost_team %}
Team that owns the cost: a mapped team, `SHARED`, or `NEEDS_OWNER_REVIEW`.
{% enddocs %}
```

```yaml
      - name: team
        description: '{{ doc("cost_team") }}'
```

Search for an existing block before writing a new one:
`grep -rn "{% docs" models/`. Columns used once get a plain description.

## Step 4 — the rest, only where the data supports it

Use a profile (the **`data-profiling`** skill) or the upstream model's tests
as evidence. Do not assume.

- **`not_null`** where the column is required by the model's logic: every grain
  column, and every foreign key.
- **`relationships`** on every foreign key, **always paired with `not_null`**. A
  NULL foreign key passes `relationships` silently.
- **`accepted_values`** only for a closed set the business defines (status
  codes, buckets the model itself assigns), never for open-ended data.
- **Row-level rules** (`dbt_utils.expression_is_true`,
  `dbt_utils.accepted_range`) only when the rule is a business rule, not an
  observation of today's data.

## Verify

```bash
uv run dbt parse                                    # yml valid, doc() references resolve
uv run dbt build --select fct_monthly_cost          # model + every test on it
uv run yamllint --strict models/                    # if the project lints YAML
```

Then run the step 1 grain query once more against the built model.

## Common mistakes

- **A uniqueness test on the wrong column set.** Too many columns (a superset of
  the grain, or a surrogate key built from too many inputs) and it passes while
  the model fans out. Too few and it fails on valid data, and someone deletes
  it. Step 1's minimality check catches both.
- **`unique` on a surrogate key as the only grain test.** The key is unique by
  construction, because it hashes whatever it was given. It proves nothing about
  the grain unless its inputs *are* the grain.
- **`relationships` without `not_null`.** NULLs pass it.
- **`accepted_values` written from today's data.** The next new value fails the
  build, and the test gets removed instead of updated.
- **A `{{ doc() }}` reference to a block that does not exist.** `dbt parse`
  fails. Grep for the block name first.
