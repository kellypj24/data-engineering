#!/bin/bash

# carved from this file:
# https://github.com/temporalio/docker-builds/blob/main/docker/auto-setup.sh


set -eu -o pipefail

: "${DB:=postgres12}"
: "${SKIP_DB_CREATE:=false}"

: "${POSTGRES_SEEDS:=}"
: "${POSTGRES_USER:=}"
: "${POSTGRES_PWD:=}"

: "${POSTGRES_TLS_ENABLED:=true}"
: "${POSTGRES_TLS_DISABLE_HOST_VERIFICATION:=true}"
: "${POSTGRES_TLS_CERT_FILE:=}"
: "${POSTGRES_TLS_KEY_FILE:=}"
: "${POSTGRES_TLS_CA_FILE:=}"
: "${POSTGRES_TLS_SERVER_NAME:=}"

: "${DBNAME:=temporal_db}"
: "${VISIBILITY_DBNAME:=temporal_visibility_db}"
: "${DB_PORT:=5432}"

# Elasticsearch
: "${ENABLE_ES:=false}"

die() {
    echo "$*" 1>&2
    exit 1
}

if [[ -z ${POSTGRES_SEEDS} ]]; then
    die "POSTGRES_SEEDS env must be set."
fi

if [[ -z ${POSTGRES_USER} ]]; then
    die "POSTGRES_USER env must be set."
fi

if [[ -z ${POSTGRES_PWD} ]]; then
    die "POSTGRES_PWD env must be set."
fi

wait_for_postgres() {
    until nc -z "${POSTGRES_SEEDS%%,*}" "${DB_PORT}"; do
        echo 'Waiting for PostgreSQL to startup.'
        sleep 1
    done

    echo 'PostgreSQL started.'
}

setup_postgres_schema() {

    export SQL_PASSWORD=${POSTGRES_PWD}

    POSTGRES_VERSION_DIR=v12
    SCHEMA_DIR=${TEMPORAL_HOME}/schema/postgresql/${POSTGRES_VERSION_DIR}/temporal/versioned
    # Create database only if its name is different from the user name. Otherwise PostgreSQL container itself will create database.
    if [[ ${DBNAME} != "${POSTGRES_USER}" && ${SKIP_DB_CREATE} != true ]]; then
        echo 'creating database'
        temporal-sql-tool \
            --plugin ${DB} \
            --ep "${POSTGRES_SEEDS}" \
            -u "${POSTGRES_USER}" \
            -p "${DB_PORT}" \
            --db "${DBNAME}" \
            --tls="${POSTGRES_TLS_ENABLED}" \
            --tls-disable-host-verification="${POSTGRES_TLS_DISABLE_HOST_VERIFICATION}" \
            --tls-cert-file "${POSTGRES_TLS_CERT_FILE}" \
            --tls-key-file "${POSTGRES_TLS_KEY_FILE}" \
            --tls-ca-file "${POSTGRES_TLS_CA_FILE}" \
            --tls-server-name "${POSTGRES_TLS_SERVER_NAME}" \
            create
    else
         echo 'skipping database creation'
    fi

    echo 'setting up schema'
    temporal-sql-tool \
        --plugin ${DB} \
        --ep "${POSTGRES_SEEDS}" \
        -u "${POSTGRES_USER}" \
        -p "${DB_PORT}" \
        --db "${DBNAME}" \
        --tls="${POSTGRES_TLS_ENABLED}" \
        --tls-disable-host-verification="${POSTGRES_TLS_DISABLE_HOST_VERIFICATION}" \
        --tls-cert-file "${POSTGRES_TLS_CERT_FILE}" \
        --tls-key-file "${POSTGRES_TLS_KEY_FILE}" \
        --tls-ca-file "${POSTGRES_TLS_CA_FILE}" \
        --tls-server-name "${POSTGRES_TLS_SERVER_NAME}" \
        setup-schema -v 0.0

    echo 'updating schema'
    temporal-sql-tool \
        --plugin ${DB} \
        --ep "${POSTGRES_SEEDS}" \
        -u "${POSTGRES_USER}" \
        -p "${DB_PORT}" \
        --db "${DBNAME}" \
        --tls="${POSTGRES_TLS_ENABLED}" \
        --tls-disable-host-verification="${POSTGRES_TLS_DISABLE_HOST_VERIFICATION}" \
        --tls-cert-file "${POSTGRES_TLS_CERT_FILE}" \
        --tls-key-file "${POSTGRES_TLS_KEY_FILE}" \
        --tls-ca-file "${POSTGRES_TLS_CA_FILE}" \
        --tls-server-name "${POSTGRES_TLS_SERVER_NAME}" \
        update-schema -d "${SCHEMA_DIR}"

    # Only setup visibility schema if ES is not enabled
    if [[ ${ENABLE_ES} == false ]]; then
      echo 'setting up visibility database'
      VISIBILITY_SCHEMA_DIR=${TEMPORAL_HOME}/schema/postgresql/${POSTGRES_VERSION_DIR}/visibility/versioned
      if [[ ${VISIBILITY_DBNAME} != "${POSTGRES_USER}" && ${SKIP_DB_CREATE} != true ]]; then
          temporal-sql-tool \
              --plugin ${DB} \
              --ep "${POSTGRES_SEEDS}" \
              -u "${POSTGRES_USER}" \
              -p "${DB_PORT}" \
              --db "${VISIBILITY_DBNAME}" \
              --tls="${POSTGRES_TLS_ENABLED}" \
              --tls-disable-host-verification="${POSTGRES_TLS_DISABLE_HOST_VERIFICATION}" \
              --tls-cert-file "${POSTGRES_TLS_CERT_FILE}" \
              --tls-key-file "${POSTGRES_TLS_KEY_FILE}" \
              --tls-ca-file "${POSTGRES_TLS_CA_FILE}" \
              --tls-server-name "${POSTGRES_TLS_SERVER_NAME}" \
              create
      fi

      echo 'setting up visibility schema'
      temporal-sql-tool \
          --plugin ${DB} \
          --ep "${POSTGRES_SEEDS}" \
          -u "${POSTGRES_USER}" \
          -p "${DB_PORT}" \
          --db "${VISIBILITY_DBNAME}" \
          --tls="${POSTGRES_TLS_ENABLED}" \
          --tls-disable-host-verification="${POSTGRES_TLS_DISABLE_HOST_VERIFICATION}" \
          --tls-cert-file "${POSTGRES_TLS_CERT_FILE}" \
          --tls-key-file "${POSTGRES_TLS_KEY_FILE}" \
          --tls-ca-file "${POSTGRES_TLS_CA_FILE}" \
          --tls-server-name "${POSTGRES_TLS_SERVER_NAME}" \
          setup-schema -v 0.0

      echo 'updating visibility schema'
      temporal-sql-tool \
          --plugin ${DB} \
          --ep "${POSTGRES_SEEDS}" \
          -u "${POSTGRES_USER}" \
          -p "${DB_PORT}" \
          --db "${VISIBILITY_DBNAME}" \
          --tls="${POSTGRES_TLS_ENABLED}" \
          --tls-disable-host-verification="${POSTGRES_TLS_DISABLE_HOST_VERIFICATION}" \
          --tls-cert-file "${POSTGRES_TLS_CERT_FILE}" \
          --tls-key-file "${POSTGRES_TLS_KEY_FILE}" \
          --tls-ca-file "${POSTGRES_TLS_CA_FILE}" \
          --tls-server-name "${POSTGRES_TLS_SERVER_NAME}" \
          update-schema -d "${VISIBILITY_SCHEMA_DIR}"
    fi
}

wait_for_postgres
setup_postgres_schema

echo "done!"