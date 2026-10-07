"""Audit every uv tool's RESOLVED dependencies (its uv.lock, not the declared
ranges) for known vulnerabilities with pip-audit, and write a markdown report.

    python .github/scripts/security_audit.py --out audit.md

Tools come from the root justfile's `mod` lines, like check_wiring.py; a tool
is audited when it has a uv.lock. Exit code 0 always (report-only); writes
`has_findings=true|false` to $GITHUB_OUTPUT when set. An audit that cannot run
for a tool is reported as such, never as "clean".
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


@dataclass
class ToolResult:
    tool: str
    directory: str
    vulns: list[tuple[str, str, str, str]] = field(
        default_factory=list
    )  # name, version, id, fix
    error: str | None = None


def uv_tools(root: Path) -> list[tuple[str, str]]:
    justfile = (root / "justfile").read_text()
    return [
        (tool, directory)
        for tool, directory in re.findall(
            r"^mod\s+([\w-]+)\s+'([^']+)'", justfile, re.MULTILINE
        )
        if (root / directory / "uv.lock").exists()
    ]


def run(cmd: list[str], cwd: Path) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd, cwd=cwd, capture_output=True, text=True, timeout=600, check=False
    )


def audit_tool(root: Path, tool: str, directory: str, runner=run) -> ToolResult:
    result = ToolResult(tool, directory)
    exported = runner(
        ["uv", "export", "--frozen", "--no-emit-project", "--no-emit-local", "--quiet"],
        root / directory,
    )
    if exported.returncode != 0:
        result.error = f"uv export failed: {exported.stderr.strip()[:300]}"
        return result
    with tempfile.TemporaryDirectory() as tmp:
        requirements = Path(tmp) / "requirements.txt"
        report = Path(tmp) / "audit.json"
        requirements.write_text(exported.stdout)
        audited = runner(
            [
                "uvx",
                "pip-audit",
                "--disable-pip",
                "-r",
                str(requirements),
                "--format",
                "json",
                "-o",
                str(report),
            ],
            root / directory,
        )
        # pip-audit exits 1 when it finds vulnerabilities; anything else non-zero is a failure.
        if audited.returncode not in (0, 1) or not report.exists():
            result.error = f"pip-audit failed: {audited.stderr.strip()[-300:]}"
            return result
        data = json.loads(report.read_text())
    for dep in data.get("dependencies", []):
        for vuln in dep.get("vulns", []):
            fix = ", ".join(vuln.get("fix_versions", [])) or "no fix yet"
            entry = (dep["name"], dep["version"], vuln["id"], fix)
            if entry not in result.vulns:  # pip-audit repeats an advisory per alias
                result.vulns.append(entry)
    return result


def render(results: list[ToolResult]) -> str:
    findings = [r for r in results if r.vulns]
    errors = [r for r in results if r.error]
    lines = [
        "Daily `pip-audit` of every tool's locked dependencies (`.github/workflows/security-audit.yml`).",
        "",
    ]
    if not findings and not errors:
        lines.append(f"All {len(results)} tools clean.")
    for r in findings:
        lines += [
            f"### {r.tool} (`{r.directory}`)",
            "",
            "| Package | Locked | Advisory | Fixed in |",
            "|---|---|---|---|",
        ]
        lines += [
            f"| {name} | {version} | {vid} | {fix} |"
            for name, version, vid, fix in r.vulns
        ]
        lines.append("")
    if errors:
        lines += ["### Could not audit", ""] + [
            f"- **{r.tool}**: {r.error}" for r in errors
        ]
    lines += [
        "",
        "Fix: bump the package in that tool (`uv lock -P <pkg>==<fixed>`), or record why it is not exploitable here.",
    ]
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args(argv)
    results = [audit_tool(ROOT, tool, directory) for tool, directory in uv_tools(ROOT)]
    args.out.write_text(render(results))
    has_findings = any(r.vulns or r.error for r in results)
    if output := os.environ.get("GITHUB_OUTPUT"):
        with open(output, "a") as f:
            f.write(f"has_findings={'true' if has_findings else 'false'}\n")
    print(args.out.read_text())
    return 0


if __name__ == "__main__":
    sys.exit(main())
