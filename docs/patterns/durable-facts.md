# Durable facts: history that outlives its source

A **durable fact** is an incremental table whose history is longer than its
source's retention: usage and audit views that keep 365 days, APIs that page
back 90, CDC streams that get re-snapshotted. Once the source ages a period
out, the table is the only copy.

## The failure it prevents

`--full-refresh` rebuilds a table by re-reading its source. For a durable fact
that silently replaces real history with whatever the source still holds. There
is no error, only less data, or coarser data.

## The pattern

Worked example: `transformation/dbt/models/marts/fct_daily_order_revenue.sql`.

| Piece | How | Why |
|---|---|---|
| Refuse rebuilds | `full_refresh=false` in `config()` | `dbt build --full-refresh` leaves the table as it is |
| Bounded restatement | `var('<model>_restate_periods', 2)`; incremental runs reprocess only periods after `MAX(period) - N` | Late corrections land; older periods are final |
| Control total | `ties_to_control_total` generic test against an independent source | Proves each complete period is right, not just present |
| Header comment | States the retention mismatch | The next person knows why the model refuses a rebuild |

### The control-total test

`tests/generic/ties_to_control_total.sql` compares `SUM(column)` per period
with a control relation, such as a ledger or an invoice:

- The **control drives**: each period it contains is complete and must match
  within `tolerance`. A period the control has and the fact lacks fails.
  Periods only the fact has (the open one) are ignored.
- It **fails when zero periods are compared**. An empty control, or a join on
  mismatched types that matches nothing, cannot pass vacuously.

## Deliberately rebuilding

Confirm the source still covers the full history, then drop the table by hand
and run the model. Treat it as a migration and write it down.

## Related

- `backfill_surrogate_keys` fills a new key column in place, so adding a key
  never needs a rebuild.
