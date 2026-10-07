"""The tagging contract (E33): every model has exactly one layer tag, and every
tag is in tag_taxonomy. check_tag_contract reads dbt's own graph."""

from dbt.cli.main import dbtRunner


def check(project) -> tuple[bool, list[str]]:
    messages: list[str] = []

    def collect(event):
        if event.info.name == "JinjaLogInfo":
            messages.append(event.info.msg)

    result = dbtRunner(callbacks=[collect]).invoke(
        ["run-operation", "check_tag_contract", "--project-dir", str(project.path)]
    )
    return result.success, messages


def retag(project, old, new):
    path = project.path / "models" / "staging" / "stg_example.yml"
    text = path.read_text()
    assert old in text
    path.write_text(text.replace(old, new))


def test_the_project_complies(project):
    ok, messages = check(project)
    assert ok, messages


def test_unlisted_tag_fails(project):
    retag(project, "tags: [revenue, order]", "tags: [revenue, order, nightly]")
    ok, messages = check(project)
    assert not ok
    assert "check_tag_contract: stg_example: tag 'nightly' is not in tag_taxonomy" in messages


def test_two_layer_tags_fail(project):
    retag(project, "tags: [revenue, order]", "tags: [revenue, order, marts]")
    ok, messages = check(project)
    assert not ok
    assert any("stg_example: needs exactly one layer tag" in m for m in messages)


def test_value_on_two_axes_fails(project):
    config = project.path / "dbt_project.yml"
    text = config.read_text()
    config.write_text(text.replace("entity: [order,", "entity: [revenue, order,"))
    ok, messages = check(project)
    assert not ok
    assert any("'revenue' is listed under more than one axis" in m for m in messages)
