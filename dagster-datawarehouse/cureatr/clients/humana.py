from dagster import DefaultScheduleStatus, ScheduleDefinition, asset, define_asset_job

from cureatr.io_managers.google_drive_io_manager import (
    GoogleDriveCsvFile,
    GoogleDriveDestination,
)
from cureatr.ops.file_generation_ops import (
    op_factory_from_query,
    update_column_headers,
)
from cureatr.ops.humana_file_gen_ops import (
    add_date_generated_column,
    generate_filename_humana_dmrp_event_level_weekly,
    generate_filename_humana_dmrp_map_extract_weekly,
    generate_filename_humana_dmrp_supplemental,
    generate_filename_humana_gnd_outcomes_weekly,
)
from cureatr.sql import FileGenerationQueries


@asset(group_name="reports_weekly_humana", io_manager_key="google_drive_io_manager")
def humana_cahps_weekly_gnd_report():
    query = FileGenerationQueries.HUMANA_CAHPS_WEEKLY_GND_OUTCOMES
    df = op_factory_from_query(query)()
    df_column_headers = update_column_headers(query, df=df)()
    df_gen_date_added = add_date_generated_column(df_column_headers)
    filename = generate_filename_humana_gnd_outcomes_weekly()

    return GoogleDriveCsvFile.from_dataframe(
        destination=GoogleDriveDestination.XDRIVE_CAHPS_WEEKLY,
        name=filename,
        data_frame=df_gen_date_added,
        file_extension="txt",
        sep="|",
    )


@asset(group_name="reports_weekly_humana", io_manager_key="google_drive_io_manager")
def humana_dmrp_weekly_event_level_detail_report():
    query = FileGenerationQueries.HUMANA_DMRP_WEEKLY_EVENT_LEVEL_DETAIL
    df = op_factory_from_query(query)()
    df_column_headers = update_column_headers(query, df=df)()
    filename = generate_filename_humana_dmrp_event_level_weekly()

    return GoogleDriveCsvFile.from_dataframe(
        destination=GoogleDriveDestination.XDRIVE_DMRP_WEEKLY,
        name=filename,
        data_frame=df_column_headers,
        file_extension="csv",
        sep=",",
    )


@asset(group_name="reports_weekly_humana", io_manager_key="google_drive_io_manager")
def humana_dmrp_weekly_map_extract_report():
    query = FileGenerationQueries.HUMANA_DMRP_WEEKLY_MAP_EXTRACT
    df = op_factory_from_query(query)()
    df_column_headers = update_column_headers(query, df=df)()
    filename = generate_filename_humana_dmrp_map_extract_weekly()

    return GoogleDriveCsvFile.from_dataframe(
        destination=GoogleDriveDestination.XDRIVE_DMRP_WEEKLY,
        name=filename,
        data_frame=df_column_headers,
        file_extension="csv",
        sep=",",
    )


@asset(group_name="reports_weekly_humana", io_manager_key="google_drive_io_manager")
def humana_dmrp_weekly_map_extract_report_txt():
    query = FileGenerationQueries.HUMANA_DMRP_WEEKLY_MAP_EXTRACT
    df = op_factory_from_query(query)()
    df_column_headers = update_column_headers(query, df=df)()
    filename = generate_filename_humana_dmrp_map_extract_weekly()

    return GoogleDriveCsvFile.from_dataframe(
        destination=GoogleDriveDestination.XDRIVE_DMRP_WEEKLY,
        name=filename,
        data_frame=df_column_headers,
        file_extension="txt",
        sep="|",
    )


@asset(group_name="reports_monthly_humana", io_manager_key="google_drive_io_manager")
def humana_dmrp_supplemental_report():
    query = FileGenerationQueries.HUMANA_DMRP_SUPPLEMENTAL
    df = op_factory_from_query(query)()
    df_column_headers = update_column_headers(query, df=df)()
    filename = generate_filename_humana_dmrp_supplemental()

    return GoogleDriveCsvFile.from_dataframe(
        destination=GoogleDriveDestination.XDRIVE_DMRP_MONTHLY,
        name=filename,
        data_frame=df_column_headers,
        file_extension="txt",
        sep="|",
    )


humana_cahps_weekly_gnd_job = define_asset_job(
    name="humana_cahps_weekly_gnd_job", selection="humana_cahps_weekly_gnd_report"
)

humana_dmrp_weekly_event_level_job = define_asset_job(
    name="humana_dmrp_weekly_event_level_job", selection="humana_dmrp_weekly_event_level_detail_report"
)

humana_dmrp_weekly_map_extract_job = define_asset_job(
    name="humana_dmrp_weekly_map_extract_job", selection="humana_dmrp_weekly_map_extract_report"
)

humana_dmrp_weekly_map_extract_job_txt = define_asset_job(
    name="humana_dmrp_weekly_map_extract_job_txt", selection="humana_dmrp_weekly_map_extract_report_txt"
)

humana_dmrp_supplemental_job = define_asset_job(
    name="humana_dmrp_supplemental_job", selection="humana_dmrp_supplemental_report"
)

humana_gnd_weekly_outcomes_schedule = ScheduleDefinition(
    job=humana_cahps_weekly_gnd_job,
    cron_schedule="0 15 * * 1",
    execution_timezone="US/Eastern",
    default_status=DefaultScheduleStatus.RUNNING,
)  # runs every Monday at 3pm EST

humana_drmp_event_detail_weekly_schedule = ScheduleDefinition(
    job=humana_dmrp_weekly_event_level_job,
    cron_schedule="0 15 * * 3",
    execution_timezone="US/Eastern",
    default_status=DefaultScheduleStatus.RUNNING,
)  # runs every Wednesday at 3pm EST

humana_drmp_map_extract_weekly_schedule = ScheduleDefinition(
    job=humana_dmrp_weekly_map_extract_job,
    cron_schedule="0 15 * * 3",
    execution_timezone="US/Eastern",
    default_status=DefaultScheduleStatus.RUNNING,
)  # runs every Wednesday at 3pm EST

humana_drmp_map_extract_weekly_txt_schedule = ScheduleDefinition(
    job=humana_dmrp_weekly_map_extract_job_txt,
    cron_schedule="0 15 * * 3",
    execution_timezone="US/Eastern",
    default_status=DefaultScheduleStatus.RUNNING,
)  # runs every Wednesday at 3pm EST

humana_dmrp_supplemental_monthly_schedule = ScheduleDefinition(
    job=humana_dmrp_supplemental_job,
    cron_schedule="0 10 * * 5#1",
    execution_timezone="US/Eastern",
    default_status=DefaultScheduleStatus.RUNNING,
)  # runs on the first Friday of every month at 10a EST
