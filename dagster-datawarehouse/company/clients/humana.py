from dagster import DefaultScheduleStatus, ScheduleDefinition, asset, define_asset_job

from company.io_managers.google_drive_io_manager import (
    GoogleDriveCsvFile,
    GoogleDriveDestination,
)
from company.ops.file_generation_ops import (
    op_factory_from_query,
    update_column_headers,
)
from company.ops.client3_file_gen_ops import (
    add_date_generated_column,
    generate_filename_client3_dmrp_event_level_weekly,
    generate_filename_client3_dmrp_map_extract_weekly,
    generate_filename_client3_dmrp_supplemental,
    generate_filename_client3_gnd_outcomes_weekly,
)
from company.sql import FileGenerationQueries


@asset(group_name="reports_weekly_client3", io_manager_key="google_drive_io_manager")
def client3_cahps_weekly_gnd_report():
    query = FileGenerationQueries.client3_CAHPS_WEEKLY_GND_OUTCOMES
    df = op_factory_from_query(query)()
    df_column_headers = update_column_headers(query, df=df)()
    df_gen_date_added = add_date_generated_column(df_column_headers)
    filename = generate_filename_client3_gnd_outcomes_weekly()

    return GoogleDriveCsvFile.from_dataframe(
        destination=GoogleDriveDestination.XDRIVE_CAHPS_WEEKLY,
        name=filename,
        data_frame=df_gen_date_added,
        file_extension="txt",
        sep="|",
    )


@asset(group_name="reports_weekly_client3", io_manager_key="google_drive_io_manager")
def client3_dmrp_weekly_event_level_detail_report():
    query = FileGenerationQueries.client3_DMRP_WEEKLY_EVENT_LEVEL_DETAIL
    df = op_factory_from_query(query)()
    df_column_headers = update_column_headers(query, df=df)()
    filename = generate_filename_client3_dmrp_event_level_weekly()

    return GoogleDriveCsvFile.from_dataframe(
        destination=GoogleDriveDestination.XDRIVE_DMRP_WEEKLY,
        name=filename,
        data_frame=df_column_headers,
        file_extension="csv",
        sep=",",
    )


@asset(group_name="reports_weekly_client3", io_manager_key="google_drive_io_manager")
def client3_dmrp_weekly_map_extract_report():
    query = FileGenerationQueries.client3_DMRP_WEEKLY_MAP_EXTRACT
    df = op_factory_from_query(query)()
    df_column_headers = update_column_headers(query, df=df)()
    filename = generate_filename_client3_dmrp_map_extract_weekly()

    return GoogleDriveCsvFile.from_dataframe(
        destination=GoogleDriveDestination.XDRIVE_DMRP_WEEKLY,
        name=filename,
        data_frame=df_column_headers,
        file_extension="csv",
        sep=",",
    )


@asset(group_name="reports_weekly_client3", io_manager_key="google_drive_io_manager")
def client3_dmrp_weekly_map_extract_report_txt():
    query = FileGenerationQueries.client3_DMRP_WEEKLY_MAP_EXTRACT
    df = op_factory_from_query(query)()
    df_column_headers = update_column_headers(query, df=df)()
    filename = generate_filename_client3_dmrp_map_extract_weekly()

    return GoogleDriveCsvFile.from_dataframe(
        destination=GoogleDriveDestination.XDRIVE_DMRP_WEEKLY,
        name=filename,
        data_frame=df_column_headers,
        file_extension="txt",
        sep="|",
    )


@asset(group_name="reports_monthly_client3", io_manager_key="google_drive_io_manager")
def client3_dmrp_supplemental_report():
    query = FileGenerationQueries.client3_DMRP_SUPPLEMENTAL
    df = op_factory_from_query(query)()
    df_column_headers = update_column_headers(query, df=df)()
    filename = generate_filename_client3_dmrp_supplemental()

    return GoogleDriveCsvFile.from_dataframe(
        destination=GoogleDriveDestination.XDRIVE_DMRP_MONTHLY,
        name=filename,
        data_frame=df_column_headers,
        file_extension="txt",
        sep="|",
    )


client3_cahps_weekly_gnd_job = define_asset_job(
    name="client3_cahps_weekly_gnd_job", selection="client3_cahps_weekly_gnd_report"
)

client3_dmrp_weekly_event_level_job = define_asset_job(
    name="client3_dmrp_weekly_event_level_job", selection="client3_dmrp_weekly_event_level_detail_report"
)

client3_dmrp_weekly_map_extract_job = define_asset_job(
    name="client3_dmrp_weekly_map_extract_job", selection="client3_dmrp_weekly_map_extract_report"
)

client3_dmrp_weekly_map_extract_job_txt = define_asset_job(
    name="client3_dmrp_weekly_map_extract_job_txt", selection="client3_dmrp_weekly_map_extract_report_txt"
)

client3_dmrp_supplemental_job = define_asset_job(
    name="client3_dmrp_supplemental_job", selection="client3_dmrp_supplemental_report"
)

client3_gnd_weekly_outcomes_schedule = ScheduleDefinition(
    job=client3_cahps_weekly_gnd_job,
    cron_schedule="0 15 * * 1",
    execution_timezone="US/Eastern",
    default_status=DefaultScheduleStatus.RUNNING,
)  # runs every Monday at 3pm EST

client3_drmp_event_detail_weekly_schedule = ScheduleDefinition(
    job=client3_dmrp_weekly_event_level_job,
    cron_schedule="0 15 * * 3",
    execution_timezone="US/Eastern",
    default_status=DefaultScheduleStatus.RUNNING,
)  # runs every Wednesday at 3pm EST

client3_drmp_map_extract_weekly_schedule = ScheduleDefinition(
    job=client3_dmrp_weekly_map_extract_job,
    cron_schedule="0 15 * * 3",
    execution_timezone="US/Eastern",
    default_status=DefaultScheduleStatus.RUNNING,
)  # runs every Wednesday at 3pm EST

client3_drmp_map_extract_weekly_txt_schedule = ScheduleDefinition(
    job=client3_dmrp_weekly_map_extract_job_txt,
    cron_schedule="0 15 * * 3",
    execution_timezone="US/Eastern",
    default_status=DefaultScheduleStatus.RUNNING,
)  # runs every Wednesday at 3pm EST

client3_dmrp_supplemental_monthly_schedule = ScheduleDefinition(
    job=client3_dmrp_supplemental_job,
    cron_schedule="0 10 * * 5#1",
    execution_timezone="US/Eastern",
    default_status=DefaultScheduleStatus.RUNNING,
)  # runs on the first Friday of every month at 10a EST
