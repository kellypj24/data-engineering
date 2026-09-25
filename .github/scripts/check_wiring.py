"""Check that every tool is wired into every shared surface.

The tool list comes from the root justfile's `mod` lines. A tool with a
pyproject.toml is a uv tool; one with only *.tf files is a Terraform tool.

uv tools must appear in:
  - the root justfile aggregates (test, lint, fmt, fmt-check), for each of
    those recipes the tool's mod.just defines;
  - ci.yml: a paths-filter entry, a `test-<tool>` job running in its
    directory, a lint-matrix row or `lint-<tool>` job, and the lockfiles job;
  - dependabot.yml: a `uv` entry for its directory;
  - the README tool table.

Terraform tools must appear in the ci.yml paths-filter, in a ci.yml
`test-<tool>` job that runs `terraform fmt -check` and `terraform test` under
their directory, in a `terraform` dependabot entry, and in the README table. They are not in the root `just` aggregates,
which would make the terraform CLI a prerequisite for `just test`.

Run from anywhere: `python .github/scripts/check_wiring.py [--root DIR]`.
Exits 1 and prints one line per missing wiring, naming the tool and surface.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import yaml

AGGREGATES = ("test", "lint", "fmt", "fmt-check")


def modules(justfile: str) -> dict[str, str]:
    return dict(re.findall(r"^mod\s+([\w-]+)\s+'([^']+)'", justfile, re.MULTILINE))


def recipes(text: str) -> set[str]:
    return set(re.findall(r"^([\w-]+)(?:\s[^:]*)?:(?!=)", text, re.MULTILINE))


def aggregate_deps(justfile: str, recipe: str) -> set[str]:
    match = re.search(rf"^{re.escape(recipe)}:([^\n]*)$", justfile, re.MULTILINE)
    return set(match.group(1).split()) if match else set()


def job_dir(job: dict) -> str | None:
    return (job.get("defaults") or {}).get("run", {}).get("working-directory")


def step_text(job: dict) -> str:
    return "\n".join(str(s.get("run", "")) for s in job.get("steps", []))


def check(root: Path) -> list[str]:
    justfile = (root / "justfile").read_text()
    ci_jobs = yaml.safe_load((root / ".github/workflows/ci.yml").read_text())["jobs"]
    dependabot = yaml.safe_load((root / ".github/dependabot.yml").read_text())
    readme = (root / "README.md").read_text()

    filter_step = next(
        s
        for s in ci_jobs["detect-changes"]["steps"]
        if "filters" in (s.get("with") or {})
    )
    filter_patterns = {
        pattern
        for patterns in yaml.safe_load(filter_step["with"]["filters"]).values()
        for pattern in patterns
    }
    lint_rows = {
        row["path"]
        for row in ci_jobs.get("lint", {})
        .get("strategy", {})
        .get("matrix", {})
        .get("include", [])
    }
    lockfile_script = step_text(ci_jobs.get("lockfiles", {}))
    updates = [
        (u["package-ecosystem"], u["directory"].strip("/"))
        for u in dependabot["updates"]
    ]

    problems = []
    for tool, directory in modules(justfile).items():
        path = root / directory

        def missing(surface: str, tool: str = tool) -> None:
            problems.append(f"{tool}: not wired into {surface}")

        if f"]({directory}/)" not in readme:
            missing("the README tool table")

        if (path / "pyproject.toml").exists():
            defined = recipes((path / "mod.just").read_text())
            for recipe in AGGREGATES:
                if recipe in defined and f"{tool}::{recipe}" not in aggregate_deps(
                    justfile, recipe
                ):
                    missing(f"the root justfile `{recipe}` recipe")
            if f"{directory}/**" not in filter_patterns:
                missing("the ci.yml paths-filter")
            test_job = ci_jobs.get(f"test-{tool}")
            if test_job is None or job_dir(test_job) != directory:
                missing(f"ci.yml job `test-{tool}` (working-directory {directory})")
            if directory not in lint_rows and f"lint-{tool}" not in ci_jobs:
                missing("the ci.yml lint matrix (or a `lint-<tool>` job)")
            if not re.search(
                rf"^\s*{re.escape(directory)}(\s|\\|$)", lockfile_script, re.MULTILINE
            ):
                missing("the ci.yml lockfiles job")
            if ("uv", directory) not in updates:
                missing("dependabot.yml (package-ecosystem: uv)")

        elif any(path.rglob("*.tf")):
            if f"{directory}/**" not in filter_patterns:
                missing("the ci.yml paths-filter")
            test_job = ci_jobs.get(f"test-{tool}")
            if test_job is None or not (job_dir(test_job) or "").startswith(directory):
                missing(
                    f"ci.yml job `test-{tool}` (working-directory under {directory})"
                )
            else:
                for command in ("terraform fmt -check", "terraform test"):
                    if command not in step_text(test_job):
                        missing(f"ci.yml job `test-{tool}` running `{command}`")
            if not any(
                eco == "terraform" and d.startswith(directory) for eco, d in updates
            ):
                missing("dependabot.yml (package-ecosystem: terraform)")

        else:
            problems.append(f"{tool}: {directory} has neither pyproject.toml nor *.tf")

    return problems


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--root", type=Path, default=Path(__file__).resolve().parents[2]
    )
    problems = check(parser.parse_args().root)
    for problem in problems:
        print(problem)
    if not problems:
        print("every tool is wired into every shared surface")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
