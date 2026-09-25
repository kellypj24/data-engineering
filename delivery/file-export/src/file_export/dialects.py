"""How each warehouse writes a query's result to a file.

The engine executes DuckDB. Snowflake is statement generation only: it emits
the `COPY INTO @stage` a caller would run, and is covered by string-level
tests, not an account.
"""

from __future__ import annotations

from dataclasses import dataclass

from file_export.config import Output


def _quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


@dataclass(frozen=True)
class DuckDBDialect:
    name: str = "duckdb"

    def unload(self, query: str, destination: str, output: Output) -> str:
        if output.format == "parquet":
            options = "FORMAT parquet"
        else:
            options = (
                f"FORMAT csv, HEADER {str(output.header).lower()}, "
                f"DELIMITER {_quote(output.delimiter)}"
            )
        return f"COPY ({query}) TO {_quote(destination)} ({options})"


@dataclass(frozen=True)
class SnowflakeDialect:
    """`destination` is a path under `stage` (e.g. `@exports`)."""

    stage: str
    name: str = "snowflake"

    def unload(self, query: str, destination: str, output: Output) -> str:
        if output.format == "parquet":
            file_format = "TYPE = PARQUET"
        else:
            file_format = (
                f"TYPE = CSV FIELD_DELIMITER = {_quote(output.delimiter)} "
                "FIELD_OPTIONALLY_ENCLOSED_BY = '\"' COMPRESSION = NONE"
            )
        return (
            f"COPY INTO {self.stage}/{destination}\n"
            f"FROM ({query})\n"
            f"FILE_FORMAT = ({file_format})\n"
            f"HEADER = {str(output.header).upper()}\n"
            "SINGLE = TRUE\n"
            "OVERWRITE = TRUE"
        )
