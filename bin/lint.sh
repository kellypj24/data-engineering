#!/bin/bash

ROOT_DIR=$(cd $(dirname "${BASH_SOURCE[0]}")/../ && pwd)

set -e

PYFILES=$(find ${ROOT_DIR} -type f -name "*.py" | grep -v data-warehouse/dbt_packages)

echo black...
/site/venv/bin/black --check --diff $PYFILES
echo ruff...
/site/venv/bin/ruff $PYFILES
echo mypy...
/site/venv/bin/mypy $PYFILES
