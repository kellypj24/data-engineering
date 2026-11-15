"""Python version pins: every project names the interpreter it runs on.

Standard library only, and runnable alone (`python pins.py [repo]`), so a repo that
can't depend on the lab keeps a copy as `.github/scripts/check_python_pins.py`. The
canonical file is light_labs/pins.py in light-labs; `lab python status --all` reports a
copy that has drifted.

The rule: a `.python-version` (major.minor) beside every pyproject.toml, or in a
directory above it. uv, CI and a fresh worktree all read it, so nothing runs on whatever
interpreter happens to be newest (a fresh worktree of data-engineering got 3.14, where
the dagster tests fail at collection).

Errors (exit 1):
  - a pyproject.toml with no pin above it
  - a pin that isn't major.minor, or that the project's requires-python excludes
  - a Dockerfile whose Python differs from its pin, unless the line above its FROM says
    `# pin-exempt: <reason>`
  - a Python version in a CI workflow that is neither a pin, a project's requires-python
    floor (testing the floor), nor the minor after a pin (the canary)

Warnings (reported, exit 0): what makes the next migration painful.
  - a pin, a floor or an exempt Dockerfile on an end-of-life Python
  - a pin two or more minors behind the newest stable CPython uv knows
  - a requires-python floor below the pin that no CI workflow runs
"""

import re
import subprocess
import sys
import tomllib
from datetime import date
from pathlib import Path

SKIP = ("archive/", ".venv/", "node_modules/")
EXEMPT = re.compile(r"#\s*pin-exempt:\s*(.+)")
FROM = re.compile(r"^FROM\s+\S*python:?(\d+\.\d+)", re.IGNORECASE)
CI_VERSION = re.compile(
    r"(?:python-version|UV_PYTHON)\s*:\s*[\"']?(\d+\.\d+)|uv python (?:install|pin) (\d+\.\d+)"
)
CI_MATRIX = re.compile(r"^\s*python(?:-version)?\s*:\s*\[([^\]]+)\]", re.MULTILINE)
SPEC = re.compile(r"(>=|<=|==|~=|!=|>|<)\s*(\d+(?:\.\d+)*)")
# Warn when a pin is this many minors behind the newest stable release.
LAG = 2


def version(text):
    return tuple(int(n) for n in text.split(".")[:2])


def show(v):
    return f"{v[0]}.{v[1]}"


def end_of_life(v, today=None):
    """CPython 3.N stops getting security fixes in October of 2016+N (3.9: 2025)."""
    return (today or date.today()) >= date(2016 + v[1], 11, 1)


def tracked(root):
    """Tracked files, or every file when `root` isn't a git checkout."""
    done = subprocess.run(
        ["git", "-C", str(root), "ls-files"], capture_output=True, text=True, check=False
    )
    names = done.stdout.splitlines() if done.returncode == 0 else None
    if names is None:
        names = [str(p.relative_to(root)) for p in Path(root).rglob("*") if p.is_file()]
    return [n for n in names if not any(part in f"/{n}" for part in (f"/{s}" for s in SKIP))]


def pin_for(root, directory):
    """(version, file) from the nearest .python-version at or above `directory`."""
    root, here = Path(root).resolve(), (Path(root) / directory).resolve()
    while True:
        path = here / ".python-version"
        if path.is_file():
            return path.read_text().strip(), path
        if here == root or root not in here.parents:
            return None, None
        here = here.parent


def floor(spec):
    """The lowest major.minor a requires-python admits, or None."""
    lows = [version(v) for op, v in SPEC.findall(spec or "") if op in (">=", "~=", "==", ">")]
    return min(lows) if lows else None


def admits(spec, v):
    """Whether a requires-python string allows major.minor `v`."""
    for op, text in SPEC.findall(spec or ""):
        bound = version(text)
        ok = {
            ">=": v >= bound, ">": v >= bound, "<=": v <= bound, "<": v < bound,
            "==": v == bound, "~=": v >= bound and v[0] == bound[0], "!=": v != bound,
        }[op]  # fmt: skip
        if not ok:
            return False
    return True


def newest_stable(run=subprocess.run):
    """The newest final CPython uv can install, or None without uv."""
    try:
        out = run(
            ["uv", "python", "list", "--all-versions"], capture_output=True, text=True, check=False
        ).stdout
    except OSError:
        return None
    finals = re.findall(r"^cpython-(\d+)\.(\d+)\.\d+-", out, re.MULTILINE)
    return max(((int(a), int(b)) for a, b in finals), default=None)


def audit(root, newest=None, today=None):
    """{"pins": {dir: version}, "errors": [...], "warnings": [...]} for one repo."""
    root = Path(root)
    files = tracked(root)
    errors, warnings, pins, floors = [], [], {}, {}
    for name in sorted(f for f in files if Path(f).name == "pyproject.toml"):
        where = str(Path(name).parent)
        text, path = pin_for(root, where)
        if text is None:
            errors.append(f"{name}: no .python-version beside it or above it")
            continue
        if not re.fullmatch(r"\d+\.\d+", text):
            errors.append(f"{path.relative_to(root.resolve())}: {text!r} is not major.minor")
            continue
        pin = pins[where] = version(text)
        try:
            spec = (
                tomllib.loads((root / name).read_text()).get("project", {}).get("requires-python")
            )
        except tomllib.TOMLDecodeError as e:
            errors.append(f"{name}: {e}")
            continue
        if spec and not admits(spec, pin):
            errors.append(f"{name}: requires-python {spec!r} excludes its pin {show(pin)}")
        low = floor(spec)
        if low:
            floors[where] = low
            if end_of_life(low, today):
                warnings.append(f"{name}: requires-python floor {show(low)} is end-of-life")
        if end_of_life(pin, today):
            warnings.append(f"{where}: pinned to {show(pin)}, which is end-of-life")
        if newest and (newest[0], newest[1] - LAG) >= pin:
            warnings.append(
                f"{where}: pinned to {show(pin)}, {newest[1] - pin[1]} minors behind {show(newest)}"
            )
    for name in sorted(f for f in files if Path(f).name.startswith("Dockerfile")):
        lines = (root / name).read_text().splitlines()
        text, _ = pin_for(root, str(Path(name).parent))
        for i, line in enumerate(lines):
            m = FROM.match(line)
            if not m or text is None or not re.fullmatch(r"\d+\.\d+", text):
                continue
            image, pin = version(m[1]), version(text)
            exempt = EXEMPT.search(lines[i - 1]) if i else None
            if image != pin and not exempt:
                errors.append(f"{name}:{i + 1}: image Python {m[1]}, pin {text}")
            elif image != pin:
                note = "end-of-life, " if end_of_life(image, today) else ""
                warnings.append(f"{name}:{i + 1}: image Python {m[1]} ({note}exempt: {exempt[1]})")
    allowed = set(pins.values()) | set(floors.values()) | {(p[0], p[1] + 1) for p in pins.values()}
    ran = set()
    for name in sorted(f for f in files if re.match(r"\.github/workflows/.+\.ya?ml$", f)):
        text = (root / name).read_text()
        found = [a or b for a, b in CI_VERSION.findall(text)]
        for group in CI_MATRIX.findall(text):
            found += re.findall(r"\d+\.\d+", group)
        for v in sorted(set(found)):
            ran.add(version(v))
            if pins and version(v) not in allowed:
                errors.append(f"{name}: Python {v} is not a pin, a floor or the next minor")
    # With no version named, CI runs the pin (uv reads .python-version).
    ran |= set(pins.values())
    untested = {}
    for where, low in sorted(floors.items()):
        if where in pins and low < pins[where] and low not in ran:
            untested.setdefault((low, pins[where]), []).append(where)
    for (low, pin), places in sorted(untested.items()):
        warnings.append(
            f"requires-python floor {show(low)} is never run (pin {show(pin)}), so nothing "
            f"shows it still works: {', '.join(places)}"
        )
    return {"pins": {k: show(v) for k, v in pins.items()}, "errors": errors, "warnings": warnings}


def report(root, result, out=sys.stdout):
    versions = sorted(set(result["pins"].values()))
    out.write(f"{Path(root).name}: {len(result['pins'])} project(s) pinned"
              f"{' to ' + ', '.join(versions) if versions else ''}\n")  # fmt: skip
    for kind in ("errors", "warnings"):
        for line in result[kind]:
            out.write(f"  {kind[:-1]}: {line}\n")


def main(argv=None):
    root = Path((argv or sys.argv[1:] or ["."])[0]).resolve()
    result = audit(root, newest=newest_stable())
    report(root, result)
    return 1 if result["errors"] else 0


if __name__ == "__main__":
    sys.exit(main())
