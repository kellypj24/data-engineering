"""Flag every bare `var('x')` whose name is not defined in dbt_project.yml.

    uv run python scripts/check_vars.py [--project-dir DIR]     # or: just dbt::check-vars

`dbt parse` does not render model bodies, so a removed or renamed var only
surfaces at compile, and many projects compile only against a real warehouse:
the break reaches main. This reads the source instead.

Rules:
  * `var('x')` with no default must be defined under `vars:` in
    dbt_project.yml, at the top level or scoped to this project's name. A var
    scoped to another package does not count. A var supplied only through
    --vars must still be declared (as null if nothing else fits): the
    declaration is where its meaning is written down.
  * `var('x', <default>)` is always allowed.
  * `var(some_name)` (not a literal) is ignored: it cannot be known statically.

Files: *.sql and *.yml under models/, macros/, snapshots/, tests/, seeds/,
analyses/. Never dbt_packages/. Lexed with Jinja's own lexer, so strings
(either quote style, with escapes) and {# comments #} are handled exactly as
dbt sees them, and dbt-only tags ({% test %}, {% snapshot %}) need no
definitions.

Exits 1 and prints `path:line: ...` per problem. Runs in the dbt tool's venv
(jinja2 and PyYAML come with dbt).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import jinja2
import yaml

SCANNED = ("models", "macros", "snapshots", "tests", "seeds", "analyses")
LEXER = jinja2.Environment()


def defined_vars(project_dir: Path) -> set[str]:
    project = yaml.safe_load((project_dir / "dbt_project.yml").read_text())
    name = project["name"]
    packages = {
        yaml.safe_load(p.read_text()).get("name")
        for p in (project_dir / "dbt_packages").glob("*/dbt_project.yml")
    }
    defined = set()
    for key, value in (project.get("vars") or {}).items():
        if key == name and isinstance(value, dict):
            defined |= set(value)  # scoped to this project
        elif key in packages and isinstance(value, dict):
            continue  # scoped to another package: not ours
        else:
            defined.add(key)
    return defined


def var_calls(source: str):
    """(line, name, has_default) for every var() call with a literal name."""
    tokens = [(line, kind, value) for line, kind, value in LEXER.lex(source) if kind != "whitespace"]
    for i, (line, kind, value) in enumerate(tokens):
        if kind != "name" or value != "var":
            continue
        if i > 0 and tokens[i - 1][1:] == ("operator", "."):
            continue  # something.var(...), not dbt's var
        if i + 3 >= len(tokens) or tokens[i + 1][1:] != ("operator", "(") or tokens[i + 2][1] != "string":
            continue
        name = tokens[i + 2][2][1:-1]
        yield line, name, tokens[i + 3][1:] == ("operator", ",")


def check(project_dir: Path) -> list[str]:
    defined = defined_vars(project_dir)
    problems = []
    for folder in SCANNED:
        for path in sorted((project_dir / folder).rglob("*")):
            if path.suffix not in {".sql", ".yml", ".yaml"} or not path.is_file():
                continue
            relative = path.relative_to(project_dir)
            try:
                calls = list(var_calls(path.read_text()))
            except jinja2.TemplateSyntaxError as error:
                problems.append(f"{relative}:{error.lineno}: cannot lex: {error.message}")
                continue
            for line, name, has_default in calls:
                if not has_default and name not in defined:
                    problems.append(
                        f"{relative}:{line}: var('{name}') has no default and is not defined "
                        "in dbt_project.yml vars"
                    )
    return problems


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--project-dir", type=Path, default=Path(__file__).resolve().parents[1])
    problems = check(parser.parse_args(argv).project_dir)
    print("\n".join(problems) if problems else "every bare var() is defined in dbt_project.yml")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
