#!/usr/bin/env python3
"""PreToolUse hook: deny Bash commands that bypass git hooks.

Blocks `git commit --no-verify` / `-n`, `git push --no-verify`, `git -c
core.hooksPath=...`, and the env vars that disable hook runners
(`LEFTHOOK=0`, `HUSKY=0`, `PRE_COMMIT_ALLOW_NO_CONFIG=1`, `SKIP=<ids> git
commit`). Heredoc bodies and quoted strings are removed before matching, so a
commit message that mentions --no-verify is not a bypass. `git push -n` is a
dry run, not a bypass, and passes.

Fails open: a payload it cannot parse is allowed, so a hook bug never blocks
every command.
"""

import json
import re
import sys

HEREDOC = re.compile(r"<<-?\s*(['\"]?)(\w+)\1[^\n]*\n.*?\n\s*\2\s*(?:\n|$)", re.DOTALL)
QUOTED = re.compile(r"'[^']*'|\"(?:\\.|[^\"\\])*\"")

# Each pattern is matched within one simple command (split on ; && || | and newlines).
BYPASSES = [
    (re.compile(r"\bgit\b.*\b(commit|push)\b.*\s--no-verify\b"), "--no-verify"),
    (
        re.compile(r"\bgit\b.*\bcommit\b(\s+\S+)*?\s-[a-zA-Z]*n[a-zA-Z]*\b"),
        "git commit -n",
    ),
    (re.compile(r"\bgit\b.*\s-c\s*core\.hooksPath="), "core.hooksPath override"),
    (re.compile(r"(^|\s)(LEFTHOOK|HUSKY)=(0|false)\b"), "hook runner disabled"),
    (re.compile(r"(^|\s)PRE_COMMIT_ALLOW_NO_CONFIG=1\b"), "PRE_COMMIT_ALLOW_NO_CONFIG"),
    (re.compile(r"(^|\s)SKIP=\S+\s+(\S+\s+)*git\b.*\bcommit\b"), "SKIP=... git commit"),
]

REASON = (
    "Commit hooks may not be bypassed ({}). Fix the failing check instead of "
    "skipping it; if the hook itself is wrong, fix the hook."
)


def bypass(command: str) -> str | None:
    """The name of the bypass in `command`, or None."""
    stripped = QUOTED.sub("''", HEREDOC.sub("\n", command))
    for part in re.split(r"&&|\|\||[;|\n]", stripped):
        for pattern, name in BYPASSES:
            if pattern.search(part):
                return name
    return None


def main() -> int:
    try:
        payload = json.load(sys.stdin)
        command = (
            payload["tool_input"]["command"]
            if payload.get("tool_name") == "Bash"
            else ""
        )
    except (ValueError, KeyError, TypeError):
        return 0
    name = bypass(command)
    if name:
        print(
            json.dumps(
                {
                    "hookSpecificOutput": {
                        "hookEventName": "PreToolUse",
                        "permissionDecision": "deny",
                        "permissionDecisionReason": REASON.format(name),
                    }
                }
            )
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
