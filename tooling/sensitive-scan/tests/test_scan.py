"""Each rule trips on its positive example, stays quiet on a near-miss, and
honours the per-line ignore. Sensitive-looking values are assembled at runtime
so this file does not itself trip other scanners."""

import json
import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest

from sensitive_scan.scan import (
    DEFAULT_CONFIG,
    IGNORE_MARKER,
    ConfigError,
    load_config,
    main,
    scan_paths,
    scan_text,
    validate_config,
)

UUID_VALUE = "3f2b8c1e-" + "9a4d-4e2f-8b6a-" + "1c2d3e4f5a6b"

# (rule, positive line, near-miss line)
CASES = [
    (
        "secrets/aws-access-key",
        "key = " + "AKIA" + "Q3EGRZ7NWXK4PLMB",
        "key = AKIA-not-a-key",
    ),
    (
        "secrets/private-key",
        "-----BEGIN " + "RSA PRIVATE KEY-----",
        "-----BEGIN PUBLIC KEY-----",
    ),
    (
        "secrets/github-token",
        "token: " + "ghp_" + "a1B2c3D4e5F6g7H8i9J0k1L2m3N4o5P6q7R8",
        "token: ghp_short",
    ),
    ("pii/ssn", "ssn,123-" + "45-6789", "ssn,000-12-3456 (reserved area never issued)"),
    (
        "pii/phone",
        "call (312) " + "867-5309",
        "call (312) 555-0123, a fictional number; version 1.212.555.1234",
    ),
    (
        "pii/email",
        "contact: jane.doe@" + "acme-corp.io",
        "contact: someone@example.com",
    ),
    (
        "pii/name-with-dob",
        "Maria Lopez, dob 1984-" + "03-17",
        "Release Notes, published 2026-01-05",
    ),
    (
        "identity-literals/uuid-comparison",
        f"WHERE customer_id = '{UUID_VALUE}'",
        f"WHERE order_ref = '{UUID_VALUE}'",
    ),
    ("identity-literals/dump-row", "8812345\t2026-01-05\tshipped", "id\tdate\tstatus"),
    # Near misses found by scanning this repo: an scp-style git remote,
    # single-letter placeholder emails, and a holiday name next to a date.
    (
        "pii/email",
        "owner = jane.doe@" + "acme-corp.io",
        "url = git@" + "github.com:org/repo.git",
    ),
    ("pii/email", "to: jane.doe@" + "acme-corp.io", 'emails = ["a@b.com", "c@d.com"]'),
    (
        "pii/name-with-dob",
        "name=Maria Lopez birth_date=1984-" + "03-17",
        '2010-01-18,"Birthday of Martin Luther King, Jr."',
    ),
]


def rules(text, config=None):
    return [f.rule for f in scan_text("f.txt", text, config or DEFAULT_CONFIG)]


@pytest.mark.parametrize(("rule", "positive", "near_miss"), CASES)
def test_rule_trips_on_positive(rule, positive, near_miss):
    assert rule in rules(positive)


@pytest.mark.parametrize(("rule", "positive", "near_miss"), CASES)
def test_rule_quiet_on_near_miss(rule, positive, near_miss):
    assert rule not in rules(near_miss)


@pytest.mark.parametrize(("rule", "positive", "near_miss"), CASES)
def test_rule_honours_the_per_line_ignore(rule, positive, near_miss):
    assert rules(f"{positive}  # {IGNORE_MARKER}") == []


def test_pipe_separated_dump_row():
    assert "identity-literals/dump-row" in rules(f"{UUID_VALUE}|2026-01-05|cancelled")


def test_findings_mask_the_value():
    [finding] = scan_text("f.txt", "ssn 123-" + "45-6789", DEFAULT_CONFIG)
    assert "45-67" not in str(finding)


def test_packs_are_selected_by_config():
    config = validate_config({"packs": ["secrets"]})
    assert rules("ssn 123-" + "45-6789", config) == []


@pytest.mark.parametrize("key", ["exclude", "skip_paths", "ignore_dirs", "allowlist"])
def test_directory_level_ignore_is_rejected(key):
    with pytest.raises(ConfigError, match="exemption"):
        validate_config({key: ["tests/"]})


def test_unknown_pack_and_key_rejected(tmp_path):
    with pytest.raises(ConfigError):
        validate_config({"packs": ["everything"]})
    path = tmp_path / ".sensitive-scan.toml"
    path.write_text('[sensitive-scan]\nexclude_dirs = ["fixtures/"]\n')
    assert main(["--config", str(path), "x"]) == 2


def test_config_file_selects_identity_columns(tmp_path):
    path = tmp_path / ".sensitive-scan.toml"
    path.write_text('[sensitive-scan]\nidentity_columns = ["account_id"]\n')
    config = load_config(path)
    assert "identity-literals/uuid-comparison" in rules(
        f"account_id = '{UUID_VALUE}'", config
    )
    assert "identity-literals/uuid-comparison" not in rules(
        f"customer_id = '{UUID_VALUE}'", config
    )


def test_cli_exit_codes(tmp_path):
    clean = tmp_path / "clean.sql"
    clean.write_text("SELECT 1\n")
    dirty = tmp_path / "dirty.csv"
    dirty.write_text("ssn,123-" + "45-6789\n")
    binary = tmp_path / "blob.bin"
    binary.write_bytes(b"\x00\x01" + ("123-" + "45-6789").encode())
    none = str(tmp_path / "absent.toml")
    assert main(["--config", none, str(clean), str(binary)]) == 0
    assert main(["--config", none, str(dirty)]) == 1


# -- gitleaks -------------------------------------------------------------------


def write(tmp_path, text):
    path = tmp_path / "settings.py"
    path.write_text(text)
    return str(path)


def fake_gitleaks(results, returncode=0):
    def run(cmd, **kwargs):
        report = Path(cmd[cmd.index("--report-path") + 1])
        report.write_text(json.dumps(results))
        return subprocess.CompletedProcess(cmd, returncode, "", "")

    return run


def test_secrets_defer_to_gitleaks_when_installed(tmp_path):
    path = write(tmp_path, "key = " + "AKIA" + "Q3EGRZ7NWXK4PLMB\n")
    leak = [
        {
            "StartLine": 1,
            "RuleID": "aws-access-token",
            "Secret": "AKIA" + "Q3EGRZ7NWXK4PLMB",
        }
    ]
    with (
        patch("shutil.which", return_value="/usr/bin/gitleaks"),
        patch("subprocess.run", side_effect=fake_gitleaks(leak)),
    ):
        found = scan_paths([path], DEFAULT_CONFIG)
    assert [f.rule for f in found] == ["secrets/gitleaks:aws-access-token"]


def test_gitleaks_respects_the_per_line_ignore(tmp_path):
    path = write(tmp_path, "key = " + "AKIA" + f"Q3EGRZ7NWXK4PLMB  # {IGNORE_MARKER}\n")
    leak = [{"StartLine": 1, "RuleID": "aws-access-token", "Secret": "x"}]
    with (
        patch("shutil.which", return_value="/usr/bin/gitleaks"),
        patch("subprocess.run", side_effect=fake_gitleaks(leak)),
    ):
        assert scan_paths([path], DEFAULT_CONFIG) == []


@pytest.mark.parametrize(
    "failure",
    [
        subprocess.TimeoutExpired("gitleaks", 60),
        OSError("exec format error"),
    ],
)
def test_gitleaks_failure_falls_back_to_builtin_patterns(tmp_path, failure):
    path = write(tmp_path, "key = " + "AKIA" + "Q3EGRZ7NWXK4PLMB\n")
    with (
        patch("shutil.which", return_value="/usr/bin/gitleaks"),
        patch("subprocess.run", side_effect=failure),
    ):
        found = scan_paths([path], DEFAULT_CONFIG)
    assert [f.rule for f in found] == ["secrets/aws-access-key"]
