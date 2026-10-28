"""Opt-in, advisory Claude review of the commits about to be pushed.

Registered as a pre-commit `pre-push` hook and enabled per push:

    CLAUDE_REVIEW=1 git push

Without CLAUDE_REVIEW=1 it does nothing. With it, it prints the review and
blocks the push ONLY when the model's explicit verdict line is
`VERDICT: CRITICAL`. Everything else is advice.

Invariants:
  * Fail open. A missing `claude` CLI, an auth error, a timeout, an
    unparseable reply, or any bug in this script prints a warning and allows
    the push. `main` never raises.
  * Gate what leaves the machine. Both sides of every changed file (base and
    head: a diff's removed lines come from base) are run through
    sensitive-scan (tooling/sensitive-scan) before any content is assembled;
    a file where either side trips it is withheld, and the review says which
    side. Every line sent came from a scanned, clean blob. If the scanner
    cannot be loaded, nothing is sent.
  * Bounded payload: CLAUDE_REVIEW_MAX_FILES (default 20),
    CLAUDE_REVIEW_MAX_BYTES (default 100000), CLAUDE_REVIEW_TIMEOUT seconds
    (default 180). Truncation is announced.
  * Isolated session. The model runs with no tools, no MCP servers, safe
    mode (no hooks, settings, or CLAUDE.md), and no session persistence
    (claude_command). CLAUDE_REVIEW_MODEL overrides the model. Every
    fail-open warning starts with "UNAVAILABLE (NOT A PASS):".
  * Refs come from PRE_COMMIT_FROM_REF / PRE_COMMIT_TO_REF (pre-commit consumes
    the hook's stdin). A new branch (all-zero from-ref) is diffed against its
    merge-base with the remote's default branch; a deleted branch has nothing
    to review; renames review the new path; deleted files send no content.

Stdlib only: `python3 review.py --help` works without a venv.
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

ZERO_SHA = re.compile(r"^0+$")
VERDICT = re.compile(r"^\s*VERDICT:\s*(CRITICAL|OK)\s*$", re.MULTILINE)

PROMPT = """You are reviewing a git diff before it is pushed. Report only real
problems: bugs, security issues, data loss, broken invariants. Be brief: one
line per finding, `path:line: finding`, most severe first. Do not restate the
diff. Then end with exactly one line:

VERDICT: CRITICAL   -- only if a finding must block this push (e.g. a leaked
                       credential, destructive data operation, obvious crash)
VERDICT: OK         -- otherwise

{notes}
--- DIFF ---
{diff}"""


@dataclass
class Limits:
    max_files: int = 20
    max_bytes: int = 100_000
    timeout: int = 180

    @classmethod
    def from_env(cls, env=os.environ) -> Limits:
        return cls(
            max_files=int(env.get("CLAUDE_REVIEW_MAX_FILES", cls.max_files)),
            max_bytes=int(env.get("CLAUDE_REVIEW_MAX_BYTES", cls.max_bytes)),
            timeout=int(env.get("CLAUDE_REVIEW_TIMEOUT", cls.timeout)),
        )


@dataclass
class Payload:
    diff: str = ""
    sent: list[str] = field(default_factory=list)
    scanned: list[str] = field(default_factory=list)
    withheld: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)


UNAVAILABLE = "UNAVAILABLE (NOT A PASS):"


def warn(message: str) -> None:
    print(f"claude-review: {message}", file=sys.stderr)


def unavailable(message: str) -> None:
    """A fail-open warning: the push is allowed but nothing was reviewed, so
    it must never read like a passing review."""
    warn(f"{UNAVAILABLE} {message}")


def git(*args: str) -> str:
    return subprocess.run(
        ["git", *args], capture_output=True, text=True, check=True, timeout=60
    ).stdout


# ---- refs --------------------------------------------------------------------


def default_remote_branch() -> str:
    try:
        return git("symbolic-ref", "--short", "refs/remotes/origin/HEAD").strip()
    except subprocess.CalledProcessError:
        return "origin/main"


def resolve_range(env=os.environ) -> tuple[str, str] | None:
    """(base, head) to review, or None when there is nothing to review."""
    from_ref = env.get("PRE_COMMIT_FROM_REF", "")
    to_ref = env.get("PRE_COMMIT_TO_REF", "")
    if not to_ref or ZERO_SHA.match(to_ref):
        return None  # branch deletion
    if from_ref and not ZERO_SHA.match(from_ref):
        return from_ref, to_ref
    # New branch: review what is not yet on the remote's default branch.
    base = git("merge-base", to_ref, default_remote_branch()).strip()
    return base, to_ref


# ---- the gate ------------------------------------------------------------------


def load_scanner():
    """sensitive-scan's scan_text and config, from the installed package or the
    sibling directory in this toolkit. None if neither is available."""
    try:
        from sensitive_scan.scan import load_config, scan_text  # type: ignore
    except ImportError:
        sibling = Path(__file__).resolve().parents[3] / "sensitive-scan" / "src"
        if not sibling.exists():
            return None
        sys.path.insert(0, str(sibling))
        try:
            from sensitive_scan.scan import load_config, scan_text  # type: ignore
        except ImportError:
            return None
    return scan_text, load_config(Path(".sensitive-scan.toml"))


def changed_files(base: str, head: str) -> list[tuple[str, str, str]]:
    """[(status, old_path, new_path)], status A/M/D/R/C/T. old_path is the
    path at base and new_path the path at head; they differ only for renames
    and copies."""
    files = []
    for line in git("diff", "--name-status", "-M", base, head).splitlines():
        parts = line.split("\t")
        if parts and parts[0]:
            files.append((parts[0][0], parts[1], parts[-1]))
    return files


def tripped_sides(
    status: str, old: str, new: str, base: str, head: str, scanner
) -> list[str]:
    """The sides ("base", "head") of one changed file that trip the scanner.
    A diff carries removed lines from base as well as added lines from head,
    so both sides are scanned; an added file has no base and a deleted file
    no head."""
    scan_text, config = scanner
    sides = []
    if status != "A":
        sides.append(("base", base, old))
    if status != "D":
        sides.append(("head", head, new))
    return [
        label
        for label, ref, path in sides
        if scan_text(path, git("show", f"{ref}:{path}"), config)
    ]


def build_payload(base: str, head: str, limits: Limits, scanner) -> Payload:
    payload = Payload()
    files = changed_files(base, head)
    if len(files) > limits.max_files:
        payload.notes.append(
            f"{len(files) - limits.max_files} of {len(files)} changed files not reviewed (max {limits.max_files})."
        )
        files = files[: limits.max_files]
    if scanner is None:
        payload.notes.append("sensitive-scan unavailable: nothing was sent.")
        return payload

    # Scan every file, both sides, before any content is assembled.
    clean = []
    withheld_notes = []
    for status, old, new in files:
        path = old if status == "D" else new
        tripped = tripped_sides(status, old, new, base, head, scanner)
        payload.scanned.append(path)
        if tripped:
            payload.withheld.append(path)
            withheld_notes.append(f"{path} ({' and '.join(tripped)})")
        elif status == "D":
            payload.notes.append(f"{path}: deleted (no content sent).")
        else:
            clean.append(path)

    used = 0
    for path in clean:
        file_diff = git("diff", "-M", base, head, "--", path)
        if used + len(file_diff.encode()) > limits.max_bytes:
            payload.notes.append(
                f"{path} and later files not sent: payload limit {limits.max_bytes} bytes."
            )
            break
        used += len(file_diff.encode())
        payload.diff += file_diff
        payload.sent.append(path)
    if withheld_notes:
        payload.notes.append(
            "Withheld by sensitive-scan, not sent: " + ", ".join(withheld_notes)
        )
    return payload


# ---- the model ------------------------------------------------------------------


def claude_command(env=os.environ) -> list[str]:
    """`claude -p` with nothing but the prompt: no tools, no MCP servers, no
    hooks, settings, or CLAUDE.md (safe mode), and no transcript on disk.
    Not --bare: it accepts only API-key auth, which breaks OAuth logins.
    An older CLI that rejects a flag exits non-zero, which fails open; never
    retry without the flag."""
    command = [
        "claude",
        "-p",
        "--tools",
        "",
        "--strict-mcp-config",
        "--mcp-config",
        '{"mcpServers":{}}',
        "--safe-mode",
        "--no-session-persistence",
    ]
    if env.get("CLAUDE_REVIEW_MODEL"):
        command += ["--model", env["CLAUDE_REVIEW_MODEL"]]
    return command


def ask_claude(payload: Payload, limits: Limits, env=os.environ) -> str:
    prompt = PROMPT.format(notes="\n".join(payload.notes), diff=payload.diff)
    completed = subprocess.run(
        claude_command(env),
        input=prompt,
        capture_output=True,
        text=True,
        timeout=limits.timeout,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"claude exited {completed.returncode}: {completed.stderr.strip()[:300]}"
        )
    return completed.stdout


def verdict(reply: str) -> str:
    """CRITICAL, OK, or ADVISORY when there is no explicit verdict line."""
    found = VERDICT.findall(reply)
    return found[-1] if found else "ADVISORY"


# ---- entry point ---------------------------------------------------------------


def run(env=os.environ) -> int:
    if env.get("CLAUDE_REVIEW") != "1":
        return 0
    limits = Limits.from_env(env)
    review_range = resolve_range(env)
    if review_range is None:
        return 0
    payload = build_payload(*review_range, limits, load_scanner())
    for note in payload.notes:
        warn(note)
    if not payload.sent:
        unavailable("nothing to send for review; push allowed.")
        return 0
    try:
        reply = ask_claude(payload, limits, env)
    except FileNotFoundError:
        unavailable("`claude` CLI not found; push allowed.")
        return 0
    except subprocess.TimeoutExpired:
        unavailable(f"review timed out after {limits.timeout}s; push allowed.")
        return 0
    except RuntimeError as exc:
        unavailable(f"{exc}; push allowed.")
        return 0
    print(reply)
    outcome = verdict(reply)
    if outcome == "CRITICAL":
        warn(
            "critical finding: push blocked. Fix it, or push without CLAUDE_REVIEW=1 to skip."
        )
        return 1
    if outcome == "ADVISORY":
        unavailable("no verdict line in the reply; treated as advisory. Push allowed.")
    return 0


def main(argv: list[str] | None = None) -> int:
    argparse.ArgumentParser(
        description="Opt-in Claude review of a push (CLAUDE_REVIEW=1). Blocks only on a critical verdict.",
    ).parse_args(argv)
    try:
        return run()
    except Exception as exc:  # noqa: BLE001 -- fail open: a review must never block on its own bug
        unavailable(f"review failed ({type(exc).__name__}: {exc}); push allowed.")
        return 0


if __name__ == "__main__":
    sys.exit(main())
