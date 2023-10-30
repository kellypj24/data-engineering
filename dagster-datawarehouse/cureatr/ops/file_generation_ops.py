import pandas as pd
import sqlalchemy as sa
from dagster import In, Out, op

from cureatr.sql import FileGenerationQuery
from cureatr.utils import BIGQUERY_URL, google_credentials


def op_factory_from_query(query: FileGenerationQuery):
    """Create an op dynamically based on a query name and query string.

    Args:
        query_name (str): The name of the query, used for naming the op.
        query_string (str): The SQL query string.

    Returns:
        OpDefinition: The new op.
    """

    @op(name=f"{query.name}_op", out=Out(pd.DataFrame))
    def dynamic_op():
        return load_dataframe(query.query, db_type=query.db_type, connection_str=query.connection_str)

    return dynamic_op


@op(
    ins={
        "sql": In(str),
        "db_type": In(str, default_value="redshift"),
        "connection_str": In(str, default_value="postgresql+psycopg2://"),
    },
    out=Out(pd.DataFrame),
)
def load_dataframe(sql: str, db_type: str, connection_str: str) -> pd.DataFrame:
    if db_type == "redshift":
        connection = sa.create_engine(connection_str)
    elif db_type == "bigquery":
        connection = sa.create_engine(BIGQUERY_URL, credentials_info=google_credentials())
    else:
        raise ValueError(f"Unsupported db_type: {db_type}")

    df = pd.read_sql_query(sql, connection)
    return df


def update_column_headers(query: FileGenerationQuery, df: pd.DataFrame) -> pd.DataFrame:
    """Create an op dynamically based on a report name and mapping dict to rename columns.

    Args:
        report_name (str): The name of the report, used for naming the op.
        header_mapping (dict): dict of column headers to rename.
        df (pd.DataFrame): The DataFrame to rename columns on.

    Returns:
        OpDefinition: The new op that renames columns in the DataFrame.
    """

    @op(name=f"{query.name}_headers_op", out=Out(pd.DataFrame))
    def dynamic_op():
        df.rename(columns=query.mapping, inplace=True)
        return df

    return dynamic_op
