import pandas as pd
import pendulum
from dagster import In, Out, op


@op
def generate_filename_client3_gnd_outcomes_weekly():
    today = pendulum.now().format("YYYYMMDD")
    filename = f"company_Disposition_20230516_{today}"
    return filename


@op
def generate_filename_client3_dmrp_event_level_weekly():
    today = pendulum.now().format("YYYYMMDD")
    filename = f"company_client3_EVENT_Detail_{today}"
    return filename


@op
def generate_filename_client3_dmrp_map_extract_weekly():
    today = pendulum.now().format("YYYYMMDD")
    filename = f"company_client3_MAP_Extract_{today}"
    return filename


@op
def generate_filename_client3_dmrp_supplemental():
    today = pendulum.now().format("YYYYMMDD")
    filename = f"company_client3supplemental_{today}"
    return filename


@op(ins={"df": In(pd.DataFrame)}, out=Out(pd.DataFrame))
def add_date_generated_column(df):
    today = pendulum.now().format("MM/DD/YYYY")
    df["Outcome File Creation Date"] = today
    return df
