"""block-no-verify PreToolUse hook (CI hardening #9): bypasses are denied,
normal commits and messages that only mention a bypass are not."""

import json
import subprocess
import sys
from pathlib import Path

import pytest

HOOK = Path(__file__).resolve().parents[2] / ".claude" / "hooks" / "block-no-verify.py"


def decide(payload) -> str | None:
    stdin = payload if isinstance(payload, str) else json.dumps(payload)
    result = subprocess.run(
        [sys.executable, str(HOOK)],
        input=stdin,
        capture_output=True,
        text=True,
        check=True,
    )
    if not result.stdout.strip():
        return None
    output = json.loads(result.stdout)["hookSpecificOutput"]
    assert output["hookEventName"] == "PreToolUse"
    return output["permissionDecision"]


def bash(command: str):
    return {"tool_name": "Bash", "tool_input": {"command": command}}


@pytest.mark.parametrize(
    "command",
    [
        "git commit --no-verify -m 'wip'",
        "git commit -m 'wip' -n",
        "git commit -an -m wip",
        "git push --no-verify origin HEAD",
        "cd repo && git commit --no-verify",
        "git -c core.hooksPath=/dev/null commit -m x",
        "LEFTHOOK=0 git commit -m x",
        "HUSKY=0 git push",
        "PRE_COMMIT_ALLOW_NO_CONFIG=1 git commit -m x",
        "SKIP=lint-dbt,hygiene git commit -m x",
    ],
)
def test_bypass_is_denied(command):
    assert decide(bash(command)) == "deny"


@pytest.mark.parametrize(
    "command",
    [
        "git commit -m 'fix: handle the --no-verify flag in docs'",
        'git commit -m "never use -n here"',
        "git commit -q -F - <<'EOF'\nci: block --no-verify and LEFTHOOK=0\n\nNever pass -n.\nEOF\ngit push -u origin HEAD",
        "git push -n origin HEAD",
        "git commit -m x --amend --no-edit",
        "git log -n 5",
        "grep -rn no-verify docs/",
        "uvx pre-commit run --all-files",
    ],
)
def test_normal_commands_pass(command):
    assert decide(bash(command)) is None


def test_other_tools_and_bad_payloads_pass():
    assert decide({"tool_name": "Edit", "tool_input": {"file_path": "x"}}) is None
    assert decide("not json") is None
    assert decide({"tool_name": "Bash"}) is None


def test_registered_in_committed_settings():
    settings = json.loads((HOOK.parents[1] / "settings.json").read_text())
    commands = [
        hook["command"]
        for entry in settings["hooks"]["PreToolUse"]
        if entry["matcher"] == "Bash"
        for hook in entry["hooks"]
    ]
    assert any("block-no-verify.py" in c for c in commands)
    assert HOOK.stat().st_mode & 0o111, "hook must be executable"
