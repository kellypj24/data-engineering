"""dbt deprecation grouping, on a captured `dbt parse --show-all-deprecations`
log from this repo (ANSI codes and wrapped lines included)."""

from pathlib import Path

from dbt_deprecations import NO_FILE, parse, render

SAMPLE = (Path(__file__).parent / "fixtures" / "dbt_parse_deprecations.log").read_text()
KIND = "MissingArgumentsPropertyInGenericTestDeprecation"


def test_captured_log_groups_by_type_and_file():
    found = parse(SAMPLE)
    assert len(found) == 16
    assert {d.kind for d in found} == {KIND}
    files = {d.file for d in found}
    assert "models/cost/marts/fct_monthly_cost.yml" in files
    assert NO_FILE not in files
    body = render(found)
    assert f"### {KIND} (16)" in body
    assert "| `models/cost/marts/fct_monthly_cost.yml` |" in body


def test_wrapped_message_is_joined_and_ansi_stripped():
    log = (
        "\x1b[0m10:00:00  [\x1b[33mWARNING\x1b[0m][ConfigDataPathDeprecation]: Deprecated\n"
        "functionality\nThe `data-paths` config has been renamed to `seed-paths`\n"
        "(dbt_project.yml).\n"
        "10:00:01  Done.\n"
    )
    [d] = parse(log)
    assert d.kind == "ConfigDataPathDeprecation"
    assert d.file == "dbt_project.yml"
    assert d.message.startswith("The `data-paths` config")


def test_clean_log():
    assert (
        parse("10:00:00  Running with dbt=1.12.5\n10:00:01  Performance info\n") == []
    )
    assert "no deprecations" in render([])
