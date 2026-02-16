"""Tests for dlt pipeline definitions.

Mocks HTTP requests and runs the pipeline into DuckDB for verification.
"""

from unittest.mock import MagicMock, patch

import dlt
import pytest

from pipelines.example_pipeline import (
    jsonplaceholder_comments,
    jsonplaceholder_posts,
    jsonplaceholder_source,
)


@pytest.fixture
def mock_posts():
    """Sample posts data."""
    return [
        {"userId": 1, "id": 1, "title": "Test Post 1", "body": "Body 1"},
        {"userId": 1, "id": 2, "title": "Test Post 2", "body": "Body 2"},
    ]


@pytest.fixture
def mock_comments():
    """Sample comments data."""
    return [
        {"postId": 1, "id": 1, "name": "Comment 1", "email": "a@b.com", "body": "text"},
        {"postId": 1, "id": 2, "name": "Comment 2", "email": "c@d.com", "body": "text"},
        {"postId": 2, "id": 3, "name": "Comment 3", "email": "e@f.com", "body": "text"},
    ]


class TestJsonPlaceholderResources:
    """Tests for individual dlt resources."""

    @patch("pipelines.example_pipeline.requests")
    def test_posts_resource_yields_data(self, mock_requests, mock_posts):
        """posts resource should yield JSON data from the API."""
        mock_response = MagicMock()
        mock_response.json.return_value = mock_posts
        mock_response.raise_for_status = MagicMock()
        mock_requests.get.return_value = mock_response

        results = list(jsonplaceholder_posts())
        assert len(results) == 2

    @patch("pipelines.example_pipeline.requests")
    def test_comments_resource_yields_data(self, mock_requests, mock_comments):
        """comments resource should yield JSON data from the API."""
        mock_response = MagicMock()
        mock_response.json.return_value = mock_comments
        mock_response.raise_for_status = MagicMock()
        mock_requests.get.return_value = mock_response

        results = list(jsonplaceholder_comments())
        assert len(results) == 3


class TestJsonPlaceholderPipeline:
    """Tests for the full dlt pipeline."""

    @patch("pipelines.example_pipeline.requests")
    def test_pipeline_creates_tables(self, mock_requests, mock_posts, mock_comments, tmp_path):
        """Pipeline should create posts and comments tables in DuckDB."""

        def side_effect(url, **kwargs):
            response = MagicMock()
            response.raise_for_status = MagicMock()
            if "posts" in url:
                response.json.return_value = mock_posts
            else:
                response.json.return_value = mock_comments
            return response

        mock_requests.get.side_effect = side_effect

        pipeline = dlt.pipeline(
            pipeline_name="test_pipeline",
            destination="duckdb",
            dataset_name="test_raw",
            pipelines_dir=str(tmp_path),
        )

        load_info = pipeline.run(jsonplaceholder_source())
        assert load_info is not None

        # Verify tables were created with data
        with pipeline.sql_client() as client:
            posts_count = client.execute_sql("SELECT COUNT(*) FROM posts")[0][0]
            assert posts_count == 2

            comments_count = client.execute_sql("SELECT COUNT(*) FROM comments")[0][0]
            assert comments_count == 3
