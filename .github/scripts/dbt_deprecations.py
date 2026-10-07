"""Group `dbt parse --show-all-deprecations` output by deprecation type and
by source file, as markdown for the dbt-deprecations tracker issue.

    dbt parse --show-all-deprecations 2>&1 | python dbt_deprecations.py --out body.md

Writes `has_deprecations=true|false` to $GITHUB_OUTPUT when set. Stdlib only.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

ANSI = re.compile(r"\x1b\[[0-9;]*m")
ENTRY_START = re.compile(r"^\d{2}:\d{2}:\d{2}\s+")
DEPRECATION = re.compile(r"\[(?:WARNING|WARN)\]\[(\w+Deprecation)\]")
SOURCE_FILE = re.compile(r"\(([\w./-]+\.(?:ya?ml|sql|csv|md|py))\)")
NO_FILE = "(no file named)"


@dataclass(frozen=True)
class Deprecation:
    kind: str
    file: str
    message: str


def parse(log: str) -> list[Deprecation]:
    """One Deprecation per warning. A warning's message runs from its
    timestamped line to the next timestamped line (dbt wraps long messages)."""
    entries: list[list[str]] = []
    for line in ANSI.sub("", log).splitlines():
        if ENTRY_START.match(line):
            entries.append([line])
        elif entries:
            entries[-1].append(line)
    found = []
    for lines in entries:
        kind = DEPRECATION.search(lines[0])
        if not kind:
            continue
        text = " ".join(part.strip() for part in lines[1:] if part.strip())
        # dbt wraps its "Deprecated functionality" header onto the next line.
        text = re.sub(r"^functionality\s+", "", text)
        source = SOURCE_FILE.search(text)
        found.append(
            Deprecation(kind.group(1), source.group(1) if source else NO_FILE, text)
        )
    return found


def render(deprecations: list[Deprecation]) -> str:
    if not deprecations:
        return "`dbt parse --show-all-deprecations` reports no deprecations.\n"
    by_kind: dict[str, dict[str, list[Deprecation]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for d in deprecations:
        by_kind[d.kind][d.file].append(d)
    summary = (
        f"`dbt parse --show-all-deprecations` reports **{len(deprecations)}** deprecation(s) "
        f"of {len(by_kind)} type(s). Fix them in batches by type before the dbt release that "
        "removes them (`.github/workflows/dbt-deprecations.yml`)."
    )
    lines = [summary, ""]
    for kind in sorted(
        by_kind, key=lambda k: -sum(len(v) for v in by_kind[k].values())
    ):
        files = by_kind[kind]
        lines += [f"### {kind} ({sum(len(v) for v in files.values())})", ""]
        example = next(iter(files.values()))[0].message
        lines += [f"> {example[:400]}", "", "| File | Count |", "|---|---:|"]
        lines += [f"| `{f}` | {len(v)} |" for f, v in sorted(files.items())]
        lines.append("")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("log", nargs="?", type=Path, help="log file (default: stdin)")
    args = parser.parse_args(argv)
    log = args.log.read_text() if args.log else sys.stdin.read()
    deprecations = parse(log)
    args.out.write_text(render(deprecations))
    if output := os.environ.get("GITHUB_OUTPUT"):
        with open(output, "a") as f:
            f.write(f"has_deprecations={'true' if deprecations else 'false'}\n")
    print(args.out.read_text())
    return 0


if __name__ == "__main__":
    sys.exit(main())
