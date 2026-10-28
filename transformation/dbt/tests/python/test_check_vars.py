"""scripts/check_vars.py (E43): a bare var() not defined in dbt_project.yml
fails with its file and line; defaulted vars, vars inside strings or Jinja
comments, and non-literal names do not."""

import importlib.util
import textwrap

from conftest import PROJECT_DIR

spec = importlib.util.spec_from_file_location("check_vars", PROJECT_DIR / "scripts/check_vars.py")
check_vars = importlib.util.module_from_spec(spec)
spec.loader.exec_module(check_vars)


def project(tmp_path, files, vars_yaml="vars:\n  defined: 1\n"):
    (tmp_path / "dbt_project.yml").write_text(f"name: my_project\n{vars_yaml}")
    for name, text in files.items():
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(textwrap.dedent(text))
    return check_vars.check(tmp_path)


def test_the_example_project_passes():
    assert check_vars.check(PROJECT_DIR) == []


def test_an_undefined_bare_var_fails_with_file_and_line(tmp_path):
    problems = project(tmp_path, {"models/m.sql": "SELECT 1\nFROM t\nWHERE d > '{{ var(\"missing\") }}'\n"})
    assert problems == [
        "models/m.sql:3: var('missing') has no default and is not defined in dbt_project.yml vars"
    ]


def test_defined_and_defaulted_vars_pass(tmp_path):
    assert project(tmp_path, {
        "models/m.sql": "{{ var('defined') }} {{ var('missing', 'fallback') }} {{ var(\"missing\", none) }}",
    }) == []


def test_strings_comments_and_non_literals_are_not_calls(tmp_path):
    assert project(tmp_path, {
        "macros/m.sql": """
            {# {{ var('in_a_comment') }} #}
            {% set text = "var('in_a_string')" %}
            {% set other = 'say "var(\\'escaped\\')"' %}
            {{ var(dynamic_name) }}
            {{ some.var('attribute_call') }}
        """,
    }) == []


def test_property_yaml_and_dbt_only_tags_are_scanned(tmp_path):
    problems = project(tmp_path, {
        "models/m.yml": "version: 2\nsources:\n  - name: raw\n    schema: \"{{ var('raw_schema') }}\"\n",
        "tests/generic/t.sql": "{% test t(model) %}SELECT {{ var('limit') }}{% endtest %}",
        "snapshots/s.sql": "{% snapshot s %}SELECT {{ var('since') }}{% endsnapshot %}",
    })
    assert [p.split(":")[0] + ":" + p.split("'")[1] for p in problems] == [
        "models/m.yml:raw_schema",
        "snapshots/s.sql:since",
        "tests/generic/t.sql:limit",
    ]


def test_project_scope_counts_and_other_package_scope_does_not(tmp_path):
    (tmp_path / "dbt_packages/other/dbt_project.yml").parent.mkdir(parents=True)
    (tmp_path / "dbt_packages/other/dbt_project.yml").write_text("name: other_package\n")
    problems = project(
        tmp_path,
        {"models/m.sql": "{{ var('ours') }} {{ var('theirs') }} {{ var('dict_var') }}"},
        vars_yaml=(
            "vars:\n"
            "  my_project:\n    ours: 1\n"
            "  other_package:\n    theirs: 1\n"
            "  dict_var:\n    prod: X\n"
        ),
    )
    assert [p.split("'")[1] for p in problems] == ["theirs"]


def test_dbt_packages_are_not_scanned(tmp_path):
    assert project(tmp_path, {"dbt_packages/pkg/models/m.sql": "{{ var('theirs') }}"}) == []
