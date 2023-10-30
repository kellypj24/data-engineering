#!/bin/bash

ROOT_DIR=$(cd $(dirname "${BASH_SOURCE[0]}")/../ && pwd)

for arg in "$@"; do
    if [ "$arg" == "--command" ]; then
        command=true
    else
        if [[ $command ]]; then
            command_args+=("$arg")
        else
            docker_args+=("$arg")
        fi
    fi
done

if [[ -f "./.local/GOOGLE_CREDENTIALS.json" ]]; then
    export GOOGLE_CREDENTIALS="$(< ./.local/GOOGLE_CREDENTIALS.json)"
fi

docker run --pull always --init --rm "${docker_args[@]}" \
    --mount="type=bind,src=${ROOT_DIR},dst=/home/dagster,consistency=cached" \
    --workdir=/home/dagster \
    --env=DAGSTER_HOME=/home/dagster/.dagster/home \
    --env=PYTHONPATH=/home/dagster \
    --env=PGHOST=${PGHOST:-bastion1-public.live.internal.aws.cureatr.com} \
    --env=PGPORT=${PGPORT:-5432} \
    --env=PGDATABASE=${PGDATABASE:-cureatr} \
    --env=PGSSLMODE=require \
    --env=PGUSER=${PGUSER:-$USER} \
    --env=PGPASSWORD=${PGPASSWORD} \
    --env=GOOGLE_CREDENTIALS \
    --env=SLACK_TOKEN \
    --env=PGSCHEMA=${PGSCHEMA:-$USER} \
    --hostname $USER-dagster.dev \
    -p 3000:3000 \
    public.ecr.aws/cureatr/datascience-dagster-base:${DAGSTER_TAG:-dev} \
    "${command_args[@]}"
