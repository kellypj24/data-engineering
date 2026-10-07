"""Release diff on fixture captures: expected counts, the invariant on an
inconsistent capture, and header validation before any diff runs."""

import csv
from pathlib import Path
from unittest.mock import patch

import pytest

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
)
from release_diff.engine import diff, main

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = Path(__file__).parent / "fixtures"
CAPTURES = FIXTURES / "captures"
PROFILE_PATH = ROOT / "profiles" / "example_products.toml"


@pytest.fixture
def profile():
    return Profile.from_toml(PROFILE_PATH.read_text())


def rows(path):
    with path.open() as f:
        return list(csv.reader(f))[1:]


def test_counts_and_reports(profile, tmp_path):
    summary = diff(
        profile,
        CAPTURES / "products_2026-01.csv",
        CAPTURES / "products_2026-02.csv",
        tmp_path,
    )
    assert (summary.added, summary.removed, summary.changed) == (2, 1, 2)
    assert (summary.old_rows, summary.new_rows) == (4, 5)
    assert [r[0] for r in rows(tmp_path / "added.csv")] == ["5", "6"]
    assert [r[0] for r in rows(tmp_path / "removed.csv")] == ["4"]
    changed = {r[0]: r for r in rows(tmp_path / "changed.csv")}
    assert set(changed) == {"2", "3"}
    assert "Check: 5 - 4 = 2 - 1." in (tmp_path / "summary.md").read_text()


def test_unchanged_values_compare_as_delivered(profile, tmp_path):
    """all_varchar: '4.50' stays '4.50', so Gizmo's price is not a change."""
    diff(
        profile,
        CAPTURES / "products_2026-01.csv",
        CAPTURES / "products_2026-02.csv",
        tmp_path,
    )
    gizmo = next(r for r in rows(tmp_path / "changed.csv") if r[0] == "3")
    assert gizmo[1:3] == ["Gizmo", "Gizmo"]  # old_name, new_name
    assert gizmo[3:5] == ["toys", "games"]


def test_inconsistent_capture_trips_the_invariant(profile, tmp_path):
    with pytest.raises(InvariantViolation, match="not unique"):
        diff(
            profile,
            CAPTURES / "products_2026-01.csv",
            FIXTURES / "products_duplicate_key.csv",
            tmp_path,
        )
    assert not any(tmp_path.iterdir())  # no partial report


def test_header_mismatch_fails_before_any_diff(profile, tmp_path):
    with (
        patch("release_diff.engine.duckdb.connect") as connect,
        pytest.raises(HeaderMismatch, match="cost"),
    ):
        diff(
            profile,
            CAPTURES / "products_2026-01.csv",
            FIXTURES / "products_renamed_column.csv",
            tmp_path,
        )
    connect.assert_not_called()


def test_invariant_arithmetic():
    ok = Summary("p", "a", "b", old_rows=10, new_rows=12, added=3, removed=1, changed=4)
    check_invariants(ok, changed_keys_on_both_sides=True)
    with pytest.raises(InvariantViolation):
        check_invariants(ok, changed_keys_on_both_sides=False)
    with pytest.raises(InvariantViolation):
        check_invariants(Summary("p", "a", "b", 10, 12, 1, 1, 0), True)


def test_resolve_pair(profile):
    names = [
        "products_2026-01.csv",
        "products_2026-03.csv",
        "products_2026-02.csv",
        "notes.txt",
    ]
    assert resolve_pair(profile, names) == (
        "products_2026-02.csv",
        "products_2026-03.csv",
    )
    assert resolve_pair(profile, names, new="products_2026-02.csv") == (
        "products_2026-01.csv",
        "products_2026-02.csv",
    )
    with pytest.raises(ValueError):
        resolve_pair(profile, ["products_2026-01.csv"])


def test_profile_validation():
    base = 'name = "p"\nexpected_headers = ["id", "v"]\n'
    with pytest.raises(ProfileError, match="safe identifier"):
        Profile.from_toml(
            base + 'key_columns = ["id; DROP TABLE x"]\ncompare_columns = ["v"]\n'
        )
    with pytest.raises(ProfileError, match="not in expected_headers"):
        Profile.from_toml(
            base + 'key_columns = ["id"]\ncompare_columns = ["missing"]\n'
        )
    with pytest.raises(ProfileError, match="both key and compare"):
        Profile.from_toml(base + 'key_columns = ["id"]\ncompare_columns = ["id"]\n')


def test_identifiers_are_quoted_and_values_escaped(profile):
    sql = build_queries(profile)["changed"]
    assert '"price"' in sql and "IS DISTINCT FROM" in sql
    assert literal("it's") == "'it''s'"


def test_cli_compares_the_latest_two_captures(tmp_path, capsys):
    out = tmp_path / "report"
    assert (
        main(
            [
                "--profile",
                str(PROFILE_PATH),
                "--captures",
                str(CAPTURES),
                "--out",
                str(out),
            ]
        )
        == 0
    )
    assert "products_2026-01.csv -> products_2026-02.csv" in capsys.readouterr().out
    assert (
        main(
            [
                "--profile",
                str(PROFILE_PATH),
                "--old",
                str(CAPTURES / "products_2026-01.csv"),
                "--new",
                str(FIXTURES / "products_duplicate_key.csv"),
                "--out",
                str(out),
            ]
        )
        == 1
    )


def test_key_columns_need_not_come_first(tmp_path):
    """Keys are looked up by name, not position."""
    toml = (
        'name = "p"\nkey_columns = ["sku"]\ncompare_columns = ["price"]\n'
        'expected_headers = ["price", "sku"]\n'
    )
    profile = Profile.from_toml(toml)
    (tmp_path / "a.csv").write_text("price,sku\n1.00,A\n2.00,B\n")
    (tmp_path / "b.csv").write_text("price,sku\n1.50,A\n3.00,C\n")
    summary = diff(profile, tmp_path / "a.csv", tmp_path / "b.csv", tmp_path / "out")
    assert (summary.added, summary.removed, summary.changed) == (1, 1, 1)
