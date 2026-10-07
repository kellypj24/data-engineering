"""Pure logic for comparing two captures of one dataset. Stdlib only, no I/O.

A *profile* declares a dataset: its key columns, the columns whose change
counts as a change, the headers a capture must have, the output sort order,
and how capture files are named. From a profile this module validates
headers, picks which pair of captures to compare, builds the SQL that the
engine runs in duckdb, and checks the result's arithmetic.

TRUST MODEL. The SQL here is built by string formatting, and static analysers
will flag it. It is safe because:
  * every IDENTIFIER (column, relation) comes from a committed profile or from
    this module, and is checked against IDENTIFIER and double-quoted;
  * every VALUE is passed through `literal()`, which escapes quotes.
Never build a profile from user input.
"""

from __future__ import annotations

import fnmatch
import re
import tomllib
from dataclasses import dataclass

IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class ProfileError(ValueError):
    """The profile itself is invalid."""


class HeaderMismatch(ValueError):
    """A capture's header does not match the profile. Raised before any diff."""


class InvariantViolation(AssertionError):
    """The diff's arithmetic does not hold; its result must not be trusted."""


def quote(identifier: str) -> str:
    if not IDENTIFIER.match(identifier):
        raise ProfileError(f"not a safe identifier: {identifier!r}")
    return f'"{identifier}"'


def literal(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


@dataclass(frozen=True)
class Profile:
    name: str
    key_columns: tuple[str, ...]
    compare_columns: tuple[str, ...]
    expected_headers: tuple[str, ...]
    sort_order: tuple[str, ...]
    capture_glob: str = "*.csv"

    @classmethod
    def from_toml(cls, text: str) -> Profile:
        raw = tomllib.loads(text)
        try:
            profile = cls(
                name=raw["name"],
                key_columns=tuple(raw["key_columns"]),
                compare_columns=tuple(raw["compare_columns"]),
                expected_headers=tuple(raw["expected_headers"]),
                sort_order=tuple(raw.get("sort_order", raw["key_columns"])),
                capture_glob=raw.get("capture_glob", "*.csv"),
            )
        except KeyError as exc:
            raise ProfileError(f"profile is missing {exc}") from exc
        profile.validate()
        return profile

    def validate(self) -> None:
        for column in (
            *self.key_columns,
            *self.compare_columns,
            *self.expected_headers,
            *self.sort_order,
        ):
            quote(column)
        if not self.key_columns:
            raise ProfileError(f"{self.name}: key_columns is empty")
        headers = set(self.expected_headers)
        for group, columns in (
            ("key_columns", self.key_columns),
            ("compare_columns", self.compare_columns),
            ("sort_order", self.sort_order),
        ):
            missing = [c for c in columns if c not in headers]
            if missing:
                raise ProfileError(
                    f"{self.name}: {group} {missing} are not in expected_headers"
                )
        overlap = set(self.key_columns) & set(self.compare_columns)
        if overlap:
            raise ProfileError(
                f"{self.name}: {sorted(overlap)} are both key and compare columns"
            )


def validate_headers(profile: Profile, which: str, headers: list[str]) -> None:
    if tuple(headers) != profile.expected_headers:
        missing = [h for h in profile.expected_headers if h not in headers]
        extra = [h for h in headers if h not in profile.expected_headers]
        raise HeaderMismatch(
            f"{profile.name}: {which} capture header does not match the profile "
            f"(missing {missing}, unexpected {extra}, expected order {list(profile.expected_headers)})"
        )


def resolve_pair(
    profile: Profile, names: list[str], old: str | None = None, new: str | None = None
) -> tuple[str, str]:
    """Which two captures to compare. Explicit names win; otherwise the two
    latest matching `capture_glob`, ordered by name (so name captures with a
    sortable release id, e.g. orders_2026-01.csv)."""
    matching = sorted(n for n in names if fnmatch.fnmatch(n, profile.capture_glob))
    if old and new:
        for name in (old, new):
            if name not in names:
                raise ValueError(f"capture {name!r} not found")
        return old, new
    if new:
        earlier = [n for n in matching if n < new]
        if not earlier:
            raise ValueError(f"no capture before {new!r} to compare against")
        return earlier[-1], new
    if len(matching) < 2:
        raise ValueError(
            f"{profile.name}: need two captures matching {profile.capture_glob!r}, found {matching}"
        )
    return matching[-2], matching[-1]


def build_queries(
    profile: Profile, old: str = "old_capture", new: str = "new_capture"
) -> dict[str, str]:
    """SQL for the engine: row counts, duplicate keys, added, removed, changed."""
    o, n = quote(old), quote(new)
    keys = [quote(c) for c in profile.key_columns]
    on = " AND ".join(f"o.{k} = n.{k}" for k in keys)
    key_list = ", ".join(keys)
    order = ", ".join(quote(c) for c in profile.sort_order)
    differs = (
        " OR ".join(
            f"o.{quote(c)} IS DISTINCT FROM n.{quote(c)}"
            for c in profile.compare_columns
        )
        or "FALSE"
    )
    changed_columns = ", ".join(
        [f"n.{k}" for k in keys]
        + [
            f"o.{quote(c)} AS {quote('old_' + c)}, n.{quote(c)} AS {quote('new_' + c)}"
            for c in profile.compare_columns
        ]
    )
    return {
        "old_rows": f"SELECT COUNT(*) FROM {o}",
        "new_rows": f"SELECT COUNT(*) FROM {n}",
        "duplicate_keys": (
            f"SELECT COUNT(*) FROM (SELECT {key_list} FROM {o} GROUP BY ALL HAVING COUNT(*) > 1) "
            f"UNION ALL SELECT COUNT(*) FROM (SELECT {key_list} FROM {n} GROUP BY ALL HAVING COUNT(*) > 1)"
        ),
        "added": f"SELECT n.* FROM {n} AS n WHERE NOT EXISTS (SELECT 1 FROM {o} AS o WHERE {on}) ORDER BY {order}",
        "removed": f"SELECT o.* FROM {o} AS o WHERE NOT EXISTS (SELECT 1 FROM {n} AS n WHERE {on}) ORDER BY {order}",
        "changed": (
            f"SELECT {changed_columns} FROM {o} AS o INNER JOIN {n} AS n ON {on} "
            f"WHERE {differs} ORDER BY {', '.join(f'n.{k}' for k in keys)}"
        ),
    }


@dataclass(frozen=True)
class Summary:
    profile: str
    old_capture: str
    new_capture: str
    old_rows: int
    new_rows: int
    added: int
    removed: int
    changed: int


def check_invariants(summary: Summary, changed_keys_on_both_sides: bool) -> None:
    """Assert the diff's arithmetic. Duplicate keys are the usual cause of a
    violation: a key-based diff cannot account for a row it cannot address."""
    expected = summary.added - summary.removed
    if summary.new_rows - summary.old_rows != expected:
        raise InvariantViolation(
            f"{summary.profile}: new_rows - old_rows = {summary.new_rows - summary.old_rows}, "
            f"but added - removed = {expected}. Keys are probably not unique."
        )
    if not changed_keys_on_both_sides:
        raise InvariantViolation(
            f"{summary.profile}: a changed row's key is missing from one side"
        )


def to_markdown(summary: Summary) -> str:
    return (
        f"# {summary.profile}: {summary.old_capture} -> {summary.new_capture}\n\n"
        "| | rows |\n|---|---:|\n"
        f"| old | {summary.old_rows} |\n| new | {summary.new_rows} |\n"
        f"| added | {summary.added} |\n| removed | {summary.removed} |\n| changed | {summary.changed} |\n\n"
        f"Check: {summary.new_rows} - {summary.old_rows} = {summary.added} - {summary.removed}.\n"
    )
