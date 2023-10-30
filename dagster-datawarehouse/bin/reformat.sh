#!/bin/bash

ROOT_DIR=$(cd $(dirname "${BASH_SOURCE[0]}")/../ && pwd)

/site/venv/bin/ruff --fix --fixable I "$ROOT_DIR"
/site/venv/bin/black "$ROOT_DIR"
