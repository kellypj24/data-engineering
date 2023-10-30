#!/bin/bash

ROOT_DIR=$(cd $(dirname "${BASH_SOURCE[0]}")/../ && pwd)
exec "$ROOT_DIR/bin/_docker-run.sh" --entrypoint=/bin/bash -it --command "${@}"
