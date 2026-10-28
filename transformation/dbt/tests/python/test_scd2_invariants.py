"""scd2_invariants (E39): one fixture per violation fails with exactly that
violation; a clean history with a gap passes. The fixtures are seeds written
into the throwaway project copy, so the real project gains none."""

import duckdb
from dbt.cli.main import dbtRunner

HEADER = "id,valid_from,valid_to\n"
FIXTURES = {
    # Two versions and a gap (deleted, then back): valid.
    "fx_scd2_clean": "1,2026-01-01,2026-02-01\n1,2026-02-01,2026-03-01\n1,2026-04-01,\n2,2026-01-01,\n",
    "fx_scd2_zero_length": "1,2026-01-01,2026-01-01\n1,2026-01-01,\n",
    "fx_scd2_open_twice": "1,2026-01-01,\n1,2026-02-01,\n",
    "fx_scd2_overlap": "1,2026-01-01,2026-03-01\n1,2026-02-01,\n",
}
SEEDS_YML = "version: 2\nseeds:\n" + "".join(
    f"""  - name: {name}
    config:
      column_types: {{id: integer, valid_from: timestamp, valid_to: timestamp}}
    tests:
      - scd2_invariants:
          key: id
          valid_from: valid_from
          valid_to: valid_to
"""
    for name in FIXTURES
)


def build_fixtures(project) -> dict[str, list[str]]:
    """Seed each fixture and run its test; {seed: [violations]}."""
    folder = project.path / "seeds" / "fx_scd2"
    folder.mkdir()
    for name, rows in FIXTURES.items():
        (folder / f"{name}.csv").write_text(HEADER + rows)
    (folder / "_fx_scd2.yml").write_text(SEEDS_YML)

    runner = dbtRunner()
    assert runner.invoke(["seed", "--select", "path:seeds/fx_scd2", "--project-dir", str(project.path)]).success
    result = runner.invoke(
        ["test", "--select", "path:seeds/fx_scd2", "--store-failures", "--project-dir", str(project.path)]
    )
    found = {name: [] for name in FIXTURES}
    with duckdb.connect(str(project.db_path)) as con:
        for test in result.result.results:
            seed = next(name for name in FIXTURES if name in test.node.name)
            if test.failures:
                relation = test.node.relation_name
                found[seed] = sorted(row[0] for row in con.execute(f"SELECT violation FROM {relation}").fetchall())
    return found


def test_each_violation_is_caught_and_a_clean_history_passes(project):
    assert build_fixtures(project) == {
        "fx_scd2_clean": [],
        "fx_scd2_zero_length": ["zero_length"],
        "fx_scd2_open_twice": ["open_twice", "overlap"],
        "fx_scd2_overlap": ["overlap"],
    }
