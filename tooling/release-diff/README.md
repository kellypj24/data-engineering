# release-diff

Compare two captures of the same dataset (a vendor's monthly release, two
snapshots of a reference table, prod against a rebuilt candidate) and report
the rows **added**, **removed**, and **changed**, with arithmetic that is
checked rather than assumed.

```bash
release-diff --profile profiles/example_products.toml --captures path/to/releases/
release-diff --profile profiles/example_products.toml --old jan.csv --new feb.csv --out report/
```

Output in `--out` (default `release-diff-report/`): `added.csv`, `removed.csv`,
`changed.csv` (each compare column as `old_<col>` / `new_<col>`), and
`summary.md`. Exit code 1 on a profile error, a header mismatch, or a violated
invariant. In those cases no report is written.

## Profiles

One committed TOML file per dataset:

```toml
name = "products"
key_columns = ["product_id"]                      # identifies a row
compare_columns = ["name", "category", "price", "status"]  # a change in any is a change
expected_headers = ["product_id", "name", "category", "price", "status"]
sort_order = ["product_id"]
capture_glob = "products_*.csv"                   # name captures so they sort by release
```

With `--captures DIR`, the two latest captures matching `capture_glob` are
compared; `--new NAME` compares that capture with the one before it.

## What is checked

- **Headers** must equal `expected_headers`, order included, before any diff
  runs. A renamed or added column fails fast instead of reporting every row as
  changed.
- **Invariants**: `new_rows - old_rows = added - removed`, and no changed row's
  key is also added or removed. The usual cause of a violation is a key that is
  not unique in a capture. The diff is then not trustworthy, so it fails
  instead of printing numbers.
- Values compare **as delivered** (every column read as text), so `4.50` and
  `4.5` are a change, and type inference never hides one.

## Design

`release_diff/core.py` is the pure core: stdlib only, no I/O. It covers
profiles, header validation, choosing the capture pair, SQL building, invariants,
and markdown. `release_diff/engine.py` runs that SQL in duckdb and writes the
files. Its trust model is in the core's header: identifiers come only from
committed profiles and are validated and quoted; values are escaped.

## Develop

```bash
just release-diff::test      # fixture captures, no network
just release-diff::example   # diff the example captures
```
