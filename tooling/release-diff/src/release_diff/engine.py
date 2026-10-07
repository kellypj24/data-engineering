"""Run a profile's diff in duckdb and write the report: added.csv,
removed.csv, changed.csv, summary.md. All SQL comes from core.build_queries."""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

import duckdb

from release_diff.core import (
    HeaderMismatch,
    InvariantViolation,
    Profile,
    ProfileError,
    Summary,
    build_queries,
    check_invariants,
    literal,
    resolve_pair,
    to_markdown,
    validate_headers,
)


def read_header(path: Path) -> list[str]:
    with path.open(newline="") as f:
        return next(csv.reader(f), [])


def diff(profile: Profile, old_path: Path, new_path: Path, out_dir: Path) -> Summary:
    # Headers first: a mismatch must fail before any diff runs.
    validate_headers(profile, "old", read_header(old_path))
    validate_headers(profile, "new", read_header(new_path))

    con = duckdb.connect()
    for table, path in (("old_capture", old_path), ("new_capture", new_path)):
        # all_varchar: compare captures as delivered, without type inference
        # deciding that "007" equals "7".
        con.execute(
            f"CREATE TABLE {table} AS SELECT * FROM read_csv({literal(str(path))}, header = true, all_varchar = true)"
        )
    queries = build_queries(profile)

    def count(sql: str) -> int:
        return con.execute(sql).fetchone()[0]

    results = {}
    for name in ("added", "removed", "changed"):
        relation = con.execute(queries[name])
        results[name] = ([d[0] for d in relation.description], relation.fetchall())

    def keys(name: str) -> set[tuple]:
        columns, data = results[name]
        positions = [columns.index(k) for k in profile.key_columns]
        return {tuple(row[i] for i in positions) for row in data}

    summary = Summary(
        profile=profile.name,
        old_capture=old_path.name,
        new_capture=new_path.name,
        old_rows=count(queries["old_rows"]),
        new_rows=count(queries["new_rows"]),
        added=len(results["added"][1]),
        removed=len(results["removed"][1]),
        changed=len(results["changed"][1]),
    )
    # Check before writing anything: a failed diff leaves no report behind.
    check_invariants(
        summary, keys("changed").isdisjoint(keys("added") | keys("removed"))
    )

    out_dir.mkdir(parents=True, exist_ok=True)
    for name, (columns, data) in results.items():
        with (out_dir / f"{name}.csv").open("w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(columns)
            writer.writerows(data)
    (out_dir / "summary.md").write_text(to_markdown(summary))
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Report rows added, removed, and changed between two captures."
    )
    parser.add_argument("--profile", type=Path, required=True)
    parser.add_argument(
        "--captures", type=Path, help="directory of captures; compares the latest two"
    )
    parser.add_argument("--old", help="old capture (path, or name within --captures)")
    parser.add_argument("--new", help="new capture (path, or name within --captures)")
    parser.add_argument("--out", type=Path, default=Path("release-diff-report"))
    args = parser.parse_args(argv)
    try:
        profile = Profile.from_toml(args.profile.read_text())
        if args.captures:
            names = [p.name for p in args.captures.iterdir()]
            old, new = resolve_pair(profile, names, args.old, args.new)
            old_path, new_path = args.captures / old, args.captures / new
        elif args.old and args.new:
            old_path, new_path = Path(args.old), Path(args.new)
        else:
            parser.error("give --captures, or both --old and --new")
        summary = diff(profile, old_path, new_path, args.out)
    except (ProfileError, HeaderMismatch, InvariantViolation, ValueError) as exc:
        print(f"release-diff: {exc}", file=sys.stderr)
        return 1
    print(to_markdown(summary))
    return 0


if __name__ == "__main__":
    sys.exit(main())
