"""Example dlt pipeline: REST API source -> DuckDB destination.

Fetches posts from JSONPlaceholder and loads them into a local DuckDB database.
"""

import dlt
import requests


@dlt.resource(name="posts", write_disposition="replace")
def jsonplaceholder_posts(base_url: str = "https://jsonplaceholder.typicode.com"):
    """Fetch posts from the JSONPlaceholder REST API."""
    response = requests.get(f"{base_url}/posts", timeout=30)
    response.raise_for_status()
    yield response.json()


@dlt.resource(name="comments", write_disposition="replace")
def jsonplaceholder_comments(base_url: str = "https://jsonplaceholder.typicode.com"):
    """Fetch comments from the JSONPlaceholder REST API."""
    response = requests.get(f"{base_url}/comments", timeout=30)
    response.raise_for_status()
    yield response.json()


@dlt.source(name="jsonplaceholder")
def jsonplaceholder_source():
    """A dlt source that groups the JSONPlaceholder resources."""
    return [jsonplaceholder_posts(), jsonplaceholder_comments()]


def run_pipeline() -> None:
    """Configure and run the pipeline."""
    pipeline = dlt.pipeline(
        pipeline_name="jsonplaceholder_pipeline",
        destination="duckdb",
        dataset_name="jsonplaceholder_raw",
    )

    load_info = pipeline.run(jsonplaceholder_source())
    print(f"Pipeline completed: {load_info}")
    print(f"Load package: {load_info.load_packages}")


if __name__ == "__main__":
    run_pipeline()
