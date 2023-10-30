import pandas as pd
import pendulum
from dagster import In, Out, op


@op
def generate_filename_humana_gnd_outcomes_weekly():
    today = pendulum.now().format("YYYYMMDD")
    filename = f"Cureatr_Disposition_20230516_{today}"
    return filename


@op
def generate_filename_humana_dmrp_event_level_weekly():
    today = pendulum.now().format("YYYYMMDD")
    filename = f"Cureatr_Humana_EVENT_Detail_{today}"
    return filename


@op
def generate_filename_humana_dmrp_map_extract_weekly():
    today = pendulum.now().format("YYYYMMDD")
    filename = f"Cureatr_Humana_MAP_Extract_{today}"
    return filename


@op
def generate_filename_humana_dmrp_supplemental():
    today = pendulum.now().format("YYYYMMDD")
    filename = f"CUREATR_humanasupplemental_{today}"
    return filename


@op(ins={"df": In(pd.DataFrame)}, out=Out(pd.DataFrame))
def add_date_generated_column(df):
    today = pendulum.now().format("MM/DD/YYYY")
    df["Outcome File Creation Date"] = today
    return df
