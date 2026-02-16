FROM apache/airflow:2.8-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc libpq-dev \
    && rm -rf /var/lib/apt/lists/*
USER airflow

COPY pyproject.toml .
RUN pip install --no-cache-dir ".[dev]"

COPY dags/ /opt/airflow/dags/
COPY plugins/ /opt/airflow/plugins/
