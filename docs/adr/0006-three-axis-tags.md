# 0006. Three-axis model tags, enforced

- Status: Accepted
- Date: 2026-10-07

## Context
Jobs selected models by path, so moving a model silently changed what ran.
Ad hoc tags mixed layer, team, cadence, and domain in one flat list, and with
no allowed list they multiplied until nobody trusted a tag selection.

## Decision
Tag every model on three independent axes: layer (set by directory in
`dbt_project.yml`), workload, and entity (both set by hand in the model's
`.yml`). The allowed values are the `tag_taxonomy` var, each value on one axis
only. `check_tag_contract` fails on a model without exactly one layer tag, or
with a tag outside the taxonomy, and runs in CI. Jobs select by tag. Details:
`docs/patterns/tagging.md`.

## Consequences
Selections compose (`tag:revenue,tag:marts`) and survive moving files. Every
new tag value is a reviewed change to `dbt_project.yml`. Folder tags accumulate,
so a directory may not mix layers: `models/cost/` was split into `staging/`,
`intermediate/`, and `marts/` to comply.
