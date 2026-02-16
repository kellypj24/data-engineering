"""Example Temporal activities for a data pipeline."""

from temporalio import activity


@activity.defn
async def extract_data(source_name: str) -> list[dict]:
    """Extract data from a source.

    In a real pipeline, this would call an API, read from a database,
    or pull from a file system.

    Args:
        source_name: Identifier for the data source.

    Returns:
        A list of raw records.
    """
    activity.logger.info(f"Extracting data from source: {source_name}")

    # Stub: return sample data
    return [
        {"id": 1, "value": "raw_record_1", "source": source_name},
        {"id": 2, "value": "raw_record_2", "source": source_name},
        {"id": 3, "value": "raw_record_3", "source": source_name},
    ]


@activity.defn
async def transform_data(raw_data: list[dict]) -> list[dict]:
    """Transform raw records.

    In a real pipeline, this would clean, validate, and reshape the data.

    Args:
        raw_data: List of raw records from the extract step.

    Returns:
        A list of transformed records.
    """
    activity.logger.info(f"Transforming {len(raw_data)} records")

    transformed = []
    for record in raw_data:
        transformed.append({
            **record,
            "value": record["value"].upper(),
            "transformed": True,
        })

    return transformed


@activity.defn
async def load_data(transformed_data: list[dict]) -> str:
    """Load transformed data into a destination.

    In a real pipeline, this would write to a database, data warehouse,
    or object storage.

    Args:
        transformed_data: List of transformed records.

    Returns:
        A summary of the load operation.
    """
    activity.logger.info(f"Loading {len(transformed_data)} records")

    # Stub: simulate loading
    record_count = len(transformed_data)
    return f"Loaded {record_count} records successfully"
