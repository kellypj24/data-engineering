FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml .
RUN pip install --no-cache-dir ".[dev]"

COPY flows/ /app/flows/
COPY blocks/ /app/blocks/
COPY schedules/ /app/schedules/

EXPOSE 4200

CMD ["prefect", "server", "start", "--host", "0.0.0.0"]
