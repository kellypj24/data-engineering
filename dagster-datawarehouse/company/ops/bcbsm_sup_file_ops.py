import datetime

import pandas as pd
import pendulum
import requests
import sqlalchemy as sa
from dagster import In, Out, op


@op(ins={"df": In(pd.DataFrame)}, out=Out(pd.DataFrame))
def fetch_taxonomy(df: pd.DataFrame) -> pd.DataFrame:
    def get_value(row):
        if row["providerkey"] is None:
            return ""
        result = requests.get(
            "https://npiregistry.cms.hhs.gov/api/", params={"number": row["providerkey"], "version": "2.1"}
        ).json()
        if "Errors" in result:
            raise Exception("Unable to fetch NPI data for: %s" % row["providerkey"])

        # Minimal checks to avoid IndexError
        if (
            result.get("results")
            and result["results"][0].get("taxonomies")
            and "code" in result["results"][0]["taxonomies"][0]
        ):
            return result["results"][0]["taxonomies"][0]["code"]
        return ""  # Return an empty string if checks fail

    df["providertaxonomy"] = df.apply(get_value, axis=1)
    cols = list(df.columns)
    cols.insert(5, cols.pop(cols.index("providertaxonomy")))
    df = df.loc[:, cols]
    return df


@op(ins={"dataframe": In(pd.DataFrame)})
def output_dataframe_to_redshift(context, dataframe: pd.DataFrame):
    # Get the last day of the prior month
    last_day_of_prior_month = datetime.date.today().replace(day=1) - datetime.timedelta(days=1)

    table_name = f"CUR_{last_day_of_prior_month.strftime('%Y%m%d')}_supplemental"
    schema_name = "datascience"

    # Create a connection to the Redshift cluster
    engine = sa.create_engine("postgresql+psycopg2://")

    # Drop the table if it already exists
    with engine.connect() as connection:
        connection.execute(f"DROP TABLE IF EXISTS {schema_name}.{table_name}")

    # Write the DataFrame to the Redshift table
    dataframe.to_sql(table_name, engine, index=False, schema=schema_name)

    context.log.info(f"DataFrame written to Redshift table {table_name} in schema '{schema_name}'")


@op
def generate_filename_client2_supplemental():
    today = pendulum.now()
    last_month = today.subtract(months=1)
    current_year = last_month.year
    month_number = last_month.month
    last_day = last_month.end_of("month").day
    filename = f"CUR_{current_year}{month_number:02d}{last_day:02d}_supplemental"
    return filename


@op
def generate_file_name_client2_statin_outreach_completed_visits():
    today = pendulum.now().format("YYYYMMDD")
    filename = f"CUR_client2_Statin_Member_Completed_Visit_Report_{today}"
    return filename


@op
def generate_file_name_client2_statin_outreach_member_disposition():
    today = pendulum.now().format("YYYYMMDD")
    filename = f"CUR_client2_Statin_Member_Disposition_Report_{today}"
    return filename
