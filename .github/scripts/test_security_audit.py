"""Security audit rendering and error handling, with pip-audit faked."""

import json
import subprocess
from pathlib import Path

from security_audit import ToolResult, audit_tool, render, uv_tools

REPO = Path(__file__).resolve().parents[2]


def fake_runner(audit_json=None, audit_code=0, export_code=0):
    def runner(cmd, cwd):
        if cmd[0] == "uv":
            return subprocess.CompletedProcess(
                cmd, export_code, "requests==2.0.0\n", "lock missing"
            )
        if audit_json is not None:
            Path(cmd[cmd.index("-o") + 1]).write_text(json.dumps(audit_json))
        return subprocess.CompletedProcess(cmd, audit_code, "", "network unreachable")

    return runner


def test_every_uv_tool_is_audited():
    tools = dict(uv_tools(REPO))
    assert tools["dbt"] == "transformation/dbt"
    assert "airbyte" not in tools  # terraform, no uv.lock


def test_vulnerability_is_reported():
    data = {
        "dependencies": [
            {
                "name": "requests",
                "version": "2.0.0",
                "vulns": [{"id": "GHSA-xxxx", "fix_versions": ["2.32.0"]}],
            }
        ]
    }
    result = audit_tool(
        REPO, "dlt", "extract_load/dlt", fake_runner(data, audit_code=1)
    )
    assert result.vulns == [("requests", "2.0.0", "GHSA-xxxx", "2.32.0")]
    body = render([result])
    assert "| requests | 2.0.0 | GHSA-xxxx | 2.32.0 |" in body


def test_a_failed_audit_is_never_reported_clean():
    result = audit_tool(REPO, "dlt", "extract_load/dlt", fake_runner(audit_code=2))
    assert result.error and "pip-audit failed" in result.error
    assert "Could not audit" in render([result])
    assert "clean" not in render([result])
    exported = audit_tool(REPO, "dlt", "extract_load/dlt", fake_runner(export_code=2))
    assert "uv export failed" in exported.error


def test_clean_run():
    assert "All 2 tools clean." in render([ToolResult("a", "a"), ToolResult("b", "b")])
