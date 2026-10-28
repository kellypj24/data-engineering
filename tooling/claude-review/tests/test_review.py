"""claude-review against a real temporary git repository; only the `claude`
subprocess is mocked, so nothing touches the network."""

import subprocess
from unittest.mock import patch

import pytest

from claude_review import review

ZERO = "0" * 40
REAL_RUN = subprocess.run


def git(repo, *args):
    return REAL_RUN(
        ["git", *args], cwd=repo, capture_output=True, text=True, check=True
    ).stdout.strip()


@pytest.fixture
def repo(tmp_path, monkeypatch):
    git(tmp_path, "init", "-q", "-b", "main")
    git(tmp_path, "config", "user.email", "dev@example.com")
    git(tmp_path, "config", "user.name", "Dev")
    (tmp_path / "app.py").write_text("def total(xs):\n    return sum(xs)\n")
    (tmp_path / "old_name.py").write_text("X = 1\n")
    (tmp_path / "legacy.py").write_text("Y = 2\n")
    git(tmp_path, "add", ".")
    git(tmp_path, "commit", "-q", "-m", "base")
    git(tmp_path, "update-ref", "refs/remotes/origin/main", "HEAD")
    monkeypatch.chdir(tmp_path)
    return tmp_path


def commit(repo, files: dict[str, str], message="change"):
    for name, text in files.items():
        (repo / name).write_text(text)
    git(repo, "add", "-A")
    git(repo, "commit", "-q", "-m", message)
    return git(repo, "rev-parse", "HEAD")


class FakeClaude:
    """Intercepts `claude -p`; every other command (git) runs for real."""

    def __init__(
        self,
        reply="looks fine\nVERDICT: OK",
        error=None,
        returncode=0,
        stderr="auth failed",
    ):
        self.reply, self.error, self.returncode = reply, error, returncode
        self.stderr = stderr
        self.prompts: list[str] = []
        self.commands: list[list[str]] = []

    def __call__(self, cmd, *args, **kwargs):
        if cmd[0] != "claude":
            return REAL_RUN(cmd, *args, **kwargs)
        self.commands.append(cmd)
        self.prompts.append(kwargs["input"])
        if self.error:
            raise self.error
        return subprocess.CompletedProcess(
            cmd, self.returncode, self.reply, self.stderr
        )


def push_env(from_ref, to_ref, **extra):
    return {
        "CLAUDE_REVIEW": "1",
        "PRE_COMMIT_FROM_REF": from_ref,
        "PRE_COMMIT_TO_REF": to_ref,
        **extra,
    }


def run(env, fake):
    with patch("subprocess.run", side_effect=fake):
        return review.run(env)


def test_does_nothing_unless_enabled(repo):
    fake = FakeClaude()
    assert run({}, fake) == 0
    assert fake.prompts == []


def test_ok_verdict_allows_and_critical_blocks(repo):
    base = git(repo, "rev-parse", "HEAD")
    head = commit(repo, {"app.py": "def total(xs):\n    return sum(xs) / len(xs)\n"})
    assert run(push_env(base, head), FakeClaude()) == 0
    critical = FakeClaude("app.py:2: divides by zero on empty input\nVERDICT: CRITICAL")
    assert run(push_env(base, head), critical) == 1
    assert "return sum(xs) / len(xs)" in critical.prompts[0]


@pytest.mark.parametrize(
    "fake",
    [
        FakeClaude(error=FileNotFoundError("claude")),
        FakeClaude(error=subprocess.TimeoutExpired("claude", 1)),
        FakeClaude(returncode=1),  # e.g. not logged in
        FakeClaude(reply="I think it is probably fine, maybe."),  # no verdict line
    ],
    ids=["missing-cli", "timeout", "auth-error", "unparseable"],
)
def test_fails_open(repo, fake, capsys):
    base = git(repo, "rev-parse", "HEAD")
    head = commit(repo, {"app.py": "print('hi')\n"})
    assert run(push_env(base, head), fake) == 0
    assert "claude-review: UNAVAILABLE (NOT A PASS):" in capsys.readouterr().err


def test_a_bug_in_the_script_fails_open(repo, monkeypatch, capsys):
    monkeypatch.setenv("CLAUDE_REVIEW", "1")
    with patch.object(review, "run", side_effect=KeyError("boom")):
        assert review.main([]) == 0
    assert "UNAVAILABLE (NOT A PASS): review failed" in capsys.readouterr().err


def test_nothing_to_send_is_not_a_pass(repo, capsys):
    base = git(repo, "rev-parse", "HEAD")
    head = commit(repo, {"app.py": "print('x')\n"})
    with patch.object(review, "load_scanner", return_value=None):
        assert run(push_env(base, head), FakeClaude()) == 0
    assert "UNAVAILABLE (NOT A PASS): nothing to send" in capsys.readouterr().err


def test_ok_verdict_has_no_unavailable_prefix(repo, capsys):
    base = git(repo, "rev-parse", "HEAD")
    head = commit(repo, {"app.py": "print('hi')\n"})
    assert run(push_env(base, head), FakeClaude()) == 0
    assert "UNAVAILABLE" not in capsys.readouterr().err


ISOLATED = [
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


def test_session_is_isolated(repo):
    base = git(repo, "rev-parse", "HEAD")
    head = commit(repo, {"app.py": "print('hi')\n"})
    fake = FakeClaude()
    assert run(push_env(base, head), fake) == 0
    assert fake.commands == [ISOLATED]


def test_model_override(repo):
    base = git(repo, "rev-parse", "HEAD")
    head = commit(repo, {"app.py": "print('hi')\n"})
    fake = FakeClaude()
    assert run(push_env(base, head, CLAUDE_REVIEW_MODEL="sonnet"), fake) == 0
    assert fake.commands == [ISOLATED + ["--model", "sonnet"]]


def test_unknown_flag_fails_open_without_retry(repo, capsys):
    base = git(repo, "rev-parse", "HEAD")
    head = commit(repo, {"app.py": "print('hi')\n"})
    fake = FakeClaude(returncode=1, stderr="error: unknown option '--safe-mode'")
    assert run(push_env(base, head), fake) == 0
    assert len(fake.commands) == 1
    err = capsys.readouterr().err
    assert "UNAVAILABLE (NOT A PASS): claude exited 1: error: unknown option" in err


def test_withheld_file_is_never_transmitted(repo):
    base = git(repo, "rev-parse", "HEAD")
    ssn = "123-" + "45-6789"
    head = commit(
        repo, {"export.csv": f"name,ssn\nx,{ssn}\n", "app.py": "print('clean')\n"}
    )
    fake = FakeClaude()
    with patch("subprocess.run", side_effect=fake):
        payload = review.build_payload(
            base, head, review.Limits(), review.load_scanner()
        )
        assert run(push_env(base, head), fake) == 0
    assert payload.withheld == ["export.csv"]
    assert set(payload.sent) <= set(payload.scanned)
    assert "export.csv" not in payload.sent
    assert ssn not in fake.prompts[0]
    assert "Withheld by sensitive-scan, not sent: export.csv" in fake.prompts[0]


def test_nothing_is_sent_without_the_scanner(repo):
    base = git(repo, "rev-parse", "HEAD")
    head = commit(repo, {"app.py": "print('x')\n"})
    fake = FakeClaude()
    with patch.object(review, "load_scanner", return_value=None):
        assert run(push_env(base, head), fake) == 0
    assert fake.prompts == []


def test_limits_truncate_with_a_notice(repo):
    base = git(repo, "rev-parse", "HEAD")
    head = commit(repo, {"a.py": "A = 1\n", "b.py": "B = 2\n", "c.py": "C = 3\n" * 200})
    fake = FakeClaude()
    assert run(push_env(base, head, CLAUDE_REVIEW_MAX_FILES="2"), fake) == 0
    assert "1 of 3 changed files not reviewed (max 2)" in fake.prompts[0]

    fake = FakeClaude()
    assert run(push_env(base, head, CLAUDE_REVIEW_MAX_BYTES="400"), fake) == 0
    assert "payload limit 400 bytes" in fake.prompts[0]
    assert "C = 3" not in fake.prompts[0]


def test_new_branch_push_resolves_its_base(repo):
    git(repo, "checkout", "-q", "-b", "feature")
    commit(repo, {"app.py": "print('feature 1')\n"})
    head = commit(repo, {"extra.py": "Z = 3\n"})
    fake = FakeClaude()
    assert run(push_env(ZERO, head), fake) == 0
    prompt = fake.prompts[0]
    assert "feature 1" in prompt and "extra.py" in prompt
    assert (
        "def total" in prompt
    )  # the base's line, shown as removed: diffed from merge-base


def test_branch_deletion_reviews_nothing(repo):
    fake = FakeClaude()
    assert run(push_env(git(repo, "rev-parse", "HEAD"), ZERO), fake) == 0
    assert fake.prompts == []


def test_rename_and_delete(repo):
    base = git(repo, "rev-parse", "HEAD")
    git(repo, "mv", "old_name.py", "new_name.py")
    git(repo, "rm", "-q", "legacy.py")
    git(repo, "commit", "-q", "-m", "rename and delete")
    head = git(repo, "rev-parse", "HEAD")
    fake = FakeClaude()
    with patch("subprocess.run", side_effect=fake):
        payload = review.build_payload(
            base, head, review.Limits(), review.load_scanner()
        )
    assert payload.sent == ["new_name.py"]
    assert "legacy.py: deleted (no content sent)." in payload.notes


def test_verdict_parsing():
    assert review.verdict("x\nVERDICT: CRITICAL\n") == "CRITICAL"
    assert review.verdict("VERDICT: OK") == "OK"
    assert review.verdict("the verdict: critical-ish") == "ADVISORY"


SSN = "123-" + "45-6789"  # split so this file does not trip the repo's own scan
FILLER = "".join(f"row,{n}\n" for n in range(20))  # keeps renames detectable


def payload_and_prompts(base, head):
    fake = FakeClaude()
    with patch("subprocess.run", side_effect=fake):
        payload = review.build_payload(
            base, head, review.Limits(), review.load_scanner()
        )
    run(push_env(base, head), fake)
    return payload, fake.prompts


def test_removing_a_secret_withholds_the_file(repo):
    base = commit(repo, {"people.csv": f"name,ssn\nx,{SSN}\n"})
    head = commit(repo, {"people.csv": "name,ssn\n", "app.py": "print('ok')\n"})
    payload, prompts = payload_and_prompts(base, head)
    assert payload.withheld == ["people.csv"]
    assert payload.sent == ["app.py"]
    assert SSN not in prompts[0]
    assert "people.csv (base)" in prompts[0]


def test_rename_whose_old_content_trips_is_withheld(repo):
    base = commit(repo, {"old.csv": FILLER + f"x,{SSN}\n"})
    git(repo, "mv", "old.csv", "new.csv")
    head = commit(repo, {"new.csv": FILLER, "app.py": "print('ok')\n"})
    assert ("R", "old.csv", "new.csv") in review.changed_files(base, head)
    payload, prompts = payload_and_prompts(base, head)
    assert payload.withheld == ["new.csv"]
    assert SSN not in prompts[0]
    assert "new.csv (base)" in prompts[0]


def test_deleting_a_tripping_file_is_reported_as_withheld(repo):
    base = commit(repo, {"people.csv": f"name,ssn\nx,{SSN}\n"})
    git(repo, "rm", "-q", "people.csv")
    head = commit(repo, {"app.py": "print('ok')\n"})
    payload, prompts = payload_and_prompts(base, head)
    assert payload.withheld == ["people.csv"]
    assert "people.csv: deleted (no content sent)." not in payload.notes
    assert SSN not in prompts[0]
    assert "Withheld by sensitive-scan, not sent: people.csv (base)" in prompts[0]


def test_scans_everything_before_assembling_anything(repo):
    base = git(repo, "rev-parse", "HEAD")
    head = commit(repo, {"a.py": "A = 1\n", "z.csv": f"x,{SSN}\n"})
    calls = []
    real = review.git

    def spy(*args):
        calls.append(args[0])
        return real(*args)

    with patch.object(review, "git", side_effect=spy):
        review.build_payload(base, head, review.Limits(), review.load_scanner())
    assert "diff" in calls[1:]
    first_diff = calls.index("diff", 1)  # calls[0] is --name-status
    assert "show" not in calls[first_diff:]
