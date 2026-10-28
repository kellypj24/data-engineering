# Period close: numbers that do not move after they are reported

A **period close** model writes one period at a time (a month of revenue, a
week of usage) and is reported from. Once a period is reported, a re-run must
reproduce it exactly, and nothing may rewrite it by accident.

## The failures it prevents

| Failure | Cause |
|---|---|
| A retry next month gives different numbers | History read as of the run date, so late-arriving rows land in a closed period |
| A closed period is rewritten | Any run may reload any period |
| Rows vanish | The delete covers more than the reload |
| The guard passes but the test fails (or the reverse) | Two copies of the guard SQL drifted |
| Two models disagree on the same period | Each has its own as-of filter |

## The pattern

| Rule | How |
|---|---|
| Read history **as of period end plus a grace window** | `valid_from <= period_end + grace` (and `valid_to > ...`), never `current_date`. The grace window (`period_close_grace_days`) lets late corrections in, then stops |
| Closed periods are locked | A period older than the open one is skipped unless listed in a reprocess var (`--vars '{reprocess_periods: [2026-09]}'`) |
| Delete scope = reload scope | Compute the period list once (a CTE or a macro) and use it for both the `DELETE` predicate and the `SELECT`. `incremental_strategy='delete+insert'` with `unique_key` = the period column does this |
| Guard before writing | A query that returns rows when the run would touch a locked period. Run it as a `pre_hook` with `assert_no_rows`, and call the same macro from a singular test |
| One shared as-of macro | The grace window and the ordering (which version wins inside a period) live in one macro every period model calls |

### `assert_no_rows`

`transformation/dbt/macros/utils/assert_no_rows.sql`. Fails the node when
`sql` returns any row, before the model writes anything:

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='period',
    pre_hook="{{ assert_no_rows(period_close_guard(this), 'closed period would be rewritten') }}",
) }}
```

```sql
-- tests/assert_period_close_guard.sql: the same SQL as the pre_hook
{{ period_close_guard(ref('fct_monthly_revenue')) }}
```

`period_close_guard` is yours to write: it returns the periods this run would
reload that are closed and not in `reprocess_periods`. Keeping it a macro is
what makes the hook and the test one rule.

## Reprocessing a closed period

List it in the reprocess var, run, and record why: a closed period changing is
a reported number changing. Treat it as a migration
([state migrations](state-migrations.md)).

## Related

- [Durable facts](durable-facts.md): the restatement window there is the same
  idea for tables whose source forgets.
- [Snapshots](snapshots.md): the type-2 history the as-of read filters.
