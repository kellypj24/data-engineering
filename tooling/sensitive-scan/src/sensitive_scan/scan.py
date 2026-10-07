"""High-precision sensitive-data scanner for commits.

For projects whose data is regulated, where secrets scanning is not enough. One
script serves as a pre-commit hook and as a CI step; pattern packs are chosen
in `.sensitive-scan.toml`:

    secrets            keys and tokens. Delegates to gitleaks when it is
                       installed; the built-in patterns are the fallback.
    pii                SSN- and phone-shaped values, email addresses, and a
                       personal name on the same line as a date of birth.
    identity-literals  a literal UUID compared against a configured identity
                       column (`where customer_id = '<uuid>'`), and tab- or
                       pipe-separated dump rows holding an identifier and a date.

Tuned for precision: a guard that cries wolf gets bypassed. Example domains
(example.com, .test, .invalid), the 555-01xx fictional phone range, reserved
SSN areas, scp-style git remotes (`user@host:path`), and one-character
placeholders (`a@b.com`) never match; a date of birth must directly follow a
DOB key.

The ONLY exemption is a `sensitive-scan:ignore` comment on the line itself, for
synthetic fixtures. There are no path or directory skips, and the config
loader rejects any key that tries to add one: directory skip lists are how real
data lands in a "tests" folder.

Stdlib only (Python 3.11+), so it runs as `python3 scan.py FILE...` with no
venv. Exit codes: 0 clean, 1 findings, 2 bad config or usage.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
import tempfile
import tomllib
from dataclasses import dataclass
from pathlib import Path

IGNORE_MARKER = "sensitive-scan:ignore"
PACKS = ("secrets", "pii", "identity-literals")
CONFIG_KEYS = {"packs", "identity_columns", "use_gitleaks"}
FORBIDDEN_KEYS = {
    "exclude",
    "excludes",
    "exclude_paths",
    "exclude_dirs",
    "ignore",
    "ignore_paths",
    "ignore_dirs",
    "skip",
    "skip_paths",
    "skip_dirs",
    "allowlist",
    "paths",
}
DEFAULT_CONFIG = {
    "packs": list(PACKS),
    "identity_columns": ["customer_id", "patient_id", "member_id", "user_id"],
    "use_gitleaks": True,
}

UUID = r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
DATE = r"\b(?:19|20)\d{2}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12]\d|3[01])\b"

SECRET_PATTERNS = {
    "aws-access-key": re.compile(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b"),
    "private-key": re.compile(
        r"-----BEGIN (?:RSA |EC |OPENSSH |DSA |ENCRYPTED )?PRIVATE KEY-----"
    ),
    "github-token": re.compile(r"\b(?:ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9]{36}\b"),
    "slack-token": re.compile(r"\bxox[abprs]-[A-Za-z0-9-]{10,}\b"),
}
SSN = re.compile(r"\b(?!000|666|9\d\d)\d{3}-(?!00)\d{2}-(?!0000)\d{4}\b")
PHONE = re.compile(r"(?<![\d.-])\(?\b(\d{3})\)?[-. ](\d{3})[-.](\d{4})\b(?![\d.-])")
EMAIL = re.compile(r"\b[A-Za-z0-9._%+-]{2,}@(?:[A-Za-z0-9-]{2,}\.)+[A-Za-z]{2,}\b(?!:)")
EXAMPLE_EMAIL_DOMAIN = re.compile(
    r"@(?:[\w-]+\.)*(?:example\.(?:com|org|net)|[\w-]+\.(?:test|invalid|example|localhost))$",
    re.IGNORECASE,
)
# The date must follow a DOB key directly ("dob: 1984-03-17", "birth_date=..."),
# so prose such as "Birthday of <holiday>, <date>" does not match.
DOB_VALUE = re.compile(
    rf"\b(?:dob|date_of_birth|birth_?date)\b[\"']?\s*[:=,]?\s*[\"']?{DATE}",
    re.IGNORECASE,
)
PERSON_NAME = re.compile(r"\b[A-Z][a-z]+ [A-Z][a-z]+\b")
DUMP_ID = re.compile(rf"(?:{UUID}|\b\d{{6,}}\b)")


class ConfigError(ValueError):
    """The config is invalid. The message says why."""


@dataclass(frozen=True)
class Finding:
    path: str
    line: int
    rule: str
    excerpt: str

    def __str__(self) -> str:
        return f"{self.path}:{self.line}: [{self.rule}] {self.excerpt}"


# ---- config -------------------------------------------------------------------


def load_config(path: Path | None) -> dict:
    """Read `.sensitive-scan.toml` (the `[sensitive-scan]` table or the top
    level). A missing file means the defaults: every pack on."""
    if path is None or not path.exists():
        return dict(DEFAULT_CONFIG)
    raw = tomllib.loads(path.read_text())
    raw = raw.get("sensitive-scan", raw)
    return validate_config(raw)


def validate_config(raw: dict) -> dict:
    forbidden = sorted(set(raw) & FORBIDDEN_KEYS)
    if forbidden:
        raise ConfigError(
            f"{forbidden}: path and directory exemptions are not supported. The only "
            f"exemption is a `{IGNORE_MARKER}` comment on the line itself."
        )
    unknown = sorted(set(raw) - CONFIG_KEYS)
    if unknown:
        raise ConfigError(
            f"unknown config keys {unknown}; known: {sorted(CONFIG_KEYS)}"
        )
    config = {**DEFAULT_CONFIG, **raw}
    bad_packs = sorted(set(config["packs"]) - set(PACKS))
    if bad_packs:
        raise ConfigError(f"unknown packs {bad_packs}; known: {list(PACKS)}")
    for column in config["identity_columns"]:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", column):
            raise ConfigError(f"identity column {column!r} is not an identifier")
    return config


# ---- rules -------------------------------------------------------------------


def mask(value: str) -> str:
    """Never echo the full sensitive value back into a terminal or CI log."""
    return (
        value[:2] + "*" * max(len(value) - 4, 1) + value[-2:]
        if len(value) > 4
        else "****"
    )


def scan_secrets(line: str) -> list[tuple[str, str]]:
    return [
        (f"secrets/{name}", m.group(0))
        for name, rx in SECRET_PATTERNS.items()
        for m in rx.finditer(line)
    ]


def scan_pii(line: str) -> list[tuple[str, str]]:
    found = [("pii/ssn", m.group(0)) for m in SSN.finditer(line)]
    for m in PHONE.finditer(line):
        exchange, number = m.group(2), m.group(3)
        if exchange == "555" and number.startswith("01"):
            continue  # 555-0100..0199 is reserved for fiction
        found.append(("pii/phone", m.group(0)))
    for m in EMAIL.finditer(line):
        if not EXAMPLE_EMAIL_DOMAIN.search(m.group(0)):
            found.append(("pii/email", m.group(0)))
    if DOB_VALUE.search(line) and PERSON_NAME.search(line):
        found.append(("pii/name-with-dob", PERSON_NAME.search(line).group(0)))
    return found


def scan_identity_literals(
    line: str, identity_columns: list[str]
) -> list[tuple[str, str]]:
    found = []
    if identity_columns:
        columns = "|".join(re.escape(c) for c in identity_columns)
        compared = re.compile(
            rf"\b(?:{columns})\b\s*(?:=|==|IN\s*\()\s*['\"]({UUID})['\"]", re.IGNORECASE
        )
        found += [
            ("identity-literals/uuid-comparison", m.group(1))
            for m in compared.finditer(line)
        ]
    for separator in ("\t", "|"):
        fields = line.split(separator)
        if len(fields) >= 3:
            ids = [f for f in fields if DUMP_ID.fullmatch(f.strip())]
            dates = [f for f in fields if re.fullmatch(DATE, f.strip())]
            if ids and dates:
                found.append(("identity-literals/dump-row", ids[0].strip()))
    return found


def scan_text(
    path: str, text: str, config: dict, builtin_secrets: bool = True
) -> list[Finding]:
    findings = []
    packs = set(config["packs"])
    for number, line in enumerate(text.splitlines(), start=1):
        if IGNORE_MARKER in line:
            continue
        hits = []
        if "secrets" in packs and builtin_secrets:
            hits += scan_secrets(line)
        if "pii" in packs:
            hits += scan_pii(line)
        if "identity-literals" in packs:
            hits += scan_identity_literals(line, config["identity_columns"])
        findings += [Finding(path, number, rule, mask(value)) for rule, value in hits]
    return findings


# ---- gitleaks ---------------------------------------------------------------------


def gitleaks_findings(
    paths: list[str], lines_by_path: dict[str, list[str]]
) -> list[Finding] | None:
    """Secrets via gitleaks. None if gitleaks is unavailable or fails, so the
    caller falls back to the built-in patterns rather than scanning nothing."""
    binary = shutil.which("gitleaks")
    if not binary:
        return None
    findings = []
    for path in paths:
        with tempfile.NamedTemporaryFile(suffix=".json") as report:
            try:
                completed = subprocess.run(
                    [
                        binary,
                        "detect",
                        "--no-git",
                        "--no-banner",
                        "--source",
                        path,
                        "--report-format",
                        "json",
                        "--report-path",
                        report.name,
                        "--exit-code",
                        "0",
                    ],
                    capture_output=True,
                    text=True,
                    timeout=60,
                    check=False,
                )
                if completed.returncode != 0:
                    return None
                results = json.loads(Path(report.name).read_text() or "[]")
            except (OSError, subprocess.SubprocessError, json.JSONDecodeError):
                return None
        for result in results:
            number = int(result.get("StartLine", 0))
            lines = lines_by_path.get(path, [])
            if 0 < number <= len(lines) and IGNORE_MARKER in lines[number - 1]:
                continue
            findings.append(
                Finding(
                    path,
                    number,
                    f"secrets/gitleaks:{result.get('RuleID', '?')}",
                    mask(str(result.get("Secret", ""))),
                )
            )
    return findings


# ---- files --------------------------------------------------------------------


def read_text(path: Path) -> str | None:
    """None for binary or unreadable files."""
    try:
        data = path.read_bytes()
    except OSError:
        return None
    if b"\x00" in data[:8192]:
        return None
    return data.decode("utf-8", errors="replace")


def tracked_files() -> list[str]:
    out = subprocess.run(
        ["git", "ls-files", "-z"], capture_output=True, check=True
    ).stdout
    return [p for p in out.decode().split("\0") if p]


def scan_paths(paths: list[str], config: dict) -> list[Finding]:
    texts = {p: t for p in paths if (t := read_text(Path(p))) is not None}
    leaks = None
    if "secrets" in config["packs"] and config.get("use_gitleaks", True):
        leaks = gitleaks_findings(
            list(texts), {p: t.splitlines() for p, t in texts.items()}
        )
    findings = list(leaks or [])
    for path, text in texts.items():
        findings += scan_text(path, text, config, builtin_secrets=leaks is None)
    return findings


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Scan files for secrets, PII, and identity literals."
    )
    parser.add_argument(
        "files", nargs="*", help="files to scan (pre-commit passes the staged ones)"
    )
    parser.add_argument(
        "--all", action="store_true", help="scan every git-tracked file"
    )
    parser.add_argument("--config", type=Path, default=Path(".sensitive-scan.toml"))
    args = parser.parse_args(argv)
    try:
        config = load_config(args.config)
    except (ConfigError, tomllib.TOMLDecodeError) as exc:
        print(f"sensitive-scan: invalid config {args.config}: {exc}", file=sys.stderr)
        return 2
    paths = tracked_files() if args.all else args.files
    findings = scan_paths(paths, config)
    for finding in findings:
        print(finding)
    if findings:
        print(
            f"\nsensitive-scan: {len(findings)} finding(s). If a value is synthetic, add "
            f"`{IGNORE_MARKER}` to that line. If it is real, do not commit it; if it is "
            "already in history, follow RUNBOOK.md.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
