# Tagging contract

Every model is tagged along three independent axes, so selections compose
instead of multiplying.

| Axis | Values | Set where | Required |
|------|--------|-----------|----------|
| **Layer** | `staging`, `intermediate`, `marts`, `validation` | by directory, in `dbt_project.yml` | exactly one |
| **Workload** | `revenue`, `delivery`, `observability`, `cost`, `data_quality` | by hand, in the model's `.yml` | optional |
| **Entity** | `order`, `customer`, `run`, `export_file`, `warehouse_spend` | by hand, in the model's `.yml` | optional |

The allowed values are the `tag_taxonomy` var in `dbt_project.yml`. Each value
belongs to one axis only, so a bare tag is unambiguous.

```yaml
models:
  - name: fct_orders
    config:
      tags: [revenue, order]  # workload, entity
```

## Selecting

A comma is an intersection, a space is a union:

```bash
dbt build --select tag:revenue,tag:marts      # revenue marts
dbt build --select tag:order                   # everything about orders, every layer
dbt build --select tag:staging tag:delivery    # all staging, plus all delivery
```

Production jobs select by tag, not by path. Moving a model between folders must
not silently drop it from a job, and a tag selection says what the job is *for*.

## Enforcement

```bash
dbt run-operation check_tag_contract
```

It fails on a model with zero or two layer tags, a tag missing from
`tag_taxonomy`, or a taxonomy value listed under two axes, and names every
violation. `tests/python/test_tag_contract.py` runs it in CI. Without
enforcement a taxonomy decays into tag soup within months.

## Changing the taxonomy

Add the value to `tag_taxonomy` in the same PR that first uses it, so the
reviewer sees the vocabulary grow. A new directory under `models/` needs a
layer tag in `dbt_project.yml`, or the check fails for every model in it. Folder
tags accumulate down the tree, so do not nest one layer folder inside another.
See [ADR 0006](../adr/0006-three-axis-tags.md).
