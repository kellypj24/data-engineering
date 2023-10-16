from dataclasses import dataclass


@dataclass
class FileGenerationQuery:
    name: str
    query: str
    mapping: dict[str, str] | None = None
    db_type: str | None = "redshift"
    connection_str: str | None = "postgresql+psycopg2://"
    filename_pattern: str | None = None

class FileGenerationQueries:
    BCBSM_SUPPLEMENTAL = FileGenerationQuery(
        name="query_name",
        query="""
        SELECT *
        FROM schema.tab;e
    """,
        mapping={
        },
    )
