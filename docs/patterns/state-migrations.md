# State migrations on promotion

Promotion moves **code**. It does not move **table state**. When a PR changes
something that deploying code does not change by itself, the PR ships a
migration file saying what to do, where, by whom, and how to check it:
`migrations/NNNN-title.md`, from `migrations/_template.md`.

Without that file, the gap is reconstructed from memory after an outage: stage
was fixed by hand weeks ago, prod was never told, and the deploy "works" until
the first incremental run.

## When a PR needs one

| The PR changes | Why the deploy is not enough | Typical action |
|----------------|------------------------------|----------------|
| a seed's CSV in a way consumers depend on now | seeds reload only when `dbt seed` runs in that environment | `dbt seed --select <seed>` per environment |
| an incremental model's columns or grain | existing rows keep the old layout; `on_schema_change` may ignore or fail | rebuild or backfill a window; never full-refresh a durable fact |
| logic whose past output is now wrong | incremental runs only touch new data | bounded backfill (the `backfill-runbook` skill) |
| a snapshot's `check_cols` or strategy | past versions are not rewritten | usually none; record it, because history now mixes rules |
| objects dbt does not manage (stage, UDF, grant) | dbt never creates them | add to the preservation manifest, then `preserve_objects` provision |
| a source's location or a schema name | old relations stay where they were | create or move, then drop the old one deliberately |

Pure code changes need nothing: a new model, a changed view, or a full-rebuild
table that the deploy rebuilds anyway.

## Rules

- **The migration is reviewed with the code.** It is part of the PR, so the
  reviewer sees the cost of the change, not only the diff.
- **Every environment gets a row**, including "none". A blank row is ambiguous;
  "none" is a decision.
- **Each action has a verify step**: a query or command whose result shows it
  is done. "Ran it" is not verification.
- **Order matters.** Say whether the action runs before or after the deploy. A
  column must exist before the code that reads it ships, and a backfill runs
  after the code that computes it correctly.
- **Back up before anything destructive.** State what was backed up and how to
  roll back, or say plainly that it cannot be undone.
- Tick the **Done** column when it is done in each environment. A migration
  file with unticked rows is unfinished work.
