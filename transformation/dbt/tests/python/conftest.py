"""Shared fixtures: run dbt in-process on a throwaway copy of the project."""

import shutil
from dataclasses import dataclass
from pathlib import Path

import pytest
from dbt.cli.main import dbtRunner

PROJECT_DIR = Path(__file__).resolve().parents[2]


@dataclass
class DbtProject:
    path: Path
    db_path: Path
    target_path: Path

    def dbt(self, *args: str) -> bool:
        """Invoke dbt against this copy; True if the command succeeded."""
        return dbtRunner().invoke([*args, "--project-dir", str(self.path)]).success


@pytest.fixture
def project(tmp_path, monkeypatch) -> DbtProject:
    """A copy of the project wired to a fresh duckdb file.

    Tests may edit the copy's files (CSV seeds, dbt_project.yml) freely.
    dbt_packages is symlinked rather than copied, so `dbt deps` must have run.
    """
    copy = tmp_path / "project"
    shutil.copytree(
        PROJECT_DIR,
        copy,
        ignore=shutil.ignore_patterns(
            ".venv", "target", "logs", "dbt_packages", "*.duckdb", "python"
        ),
    )
    (copy / "dbt_packages").symlink_to(PROJECT_DIR / "dbt_packages")
    db_path = tmp_path / "warehouse.duckdb"
    monkeypatch.setenv("DBT_PROFILES_DIR", str(copy))
    monkeypatch.setenv("DBT_TARGET", "duckdb")
    monkeypatch.setenv("DUCKDB_PATH", str(db_path))
    monkeypatch.setenv("DBT_TARGET_PATH", str(tmp_path / "target"))
    monkeypatch.setenv("DBT_LOG_PATH", str(tmp_path / "logs"))
    return DbtProject(path=copy, db_path=db_path, target_path=tmp_path / "target")
