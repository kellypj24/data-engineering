# Production Dagster image
# Build:  docker build -t dagster-orchestration .
# Run:    docker run -p 4000:4000 dagster-orchestration

FROM python:3.11-slim AS base

WORKDIR /app

# System deps needed by some Python packages (e.g. psycopg2, pyarrow).
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc libpq-dev && \
    rm -rf /var/lib/apt/lists/*

# Install Python dependencies first for better layer caching.
COPY pyproject.toml ./
RUN pip install --no-cache-dir ".[dev]"

# Copy application source.
COPY src/ ./src/
COPY workspace.yaml dagster.yaml ./

# Expose the gRPC port used by the Dagster code server.
EXPOSE 4000

# Run the Dagster gRPC code server so that dagster-webserver and dagit
# can connect to this container as a code location.
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "src"]
