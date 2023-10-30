from dagster import (
    DefaultScheduleStatus,
    ScheduleDefinition,
    asset,
    define_asset_job,
)

from company.io_managers.google_drive_io_manager import (
    GoogleDriveCsvFile,
    GoogleDriveDestination,
)
from company.ops.client2_sup_file_ops import (
    fetch_taxonomy,
    generate_file_name_client2_statin_outreach_completed_visits,
    generate_file_name_client2_statin_outreach_member_disposition,
    generate_filename_client2_supplemental,
)
from company.ops.file_generation_ops import (
    op_factory_from_query,
    update_column_headers,
)
from company.sql import FileGenerationQueries


@asset(group_name="reports_monthly_client2", io_manager_key="google_drive_io_manager")
def client2_supplemental_file_report():
    query = FileGenerationQueries.client2_SUPPLEMENTAL
    df = op_factory_from_query(query)()
    df_with_taxonomy = fetch_taxonomy(df=df)
    df_header_updated = update_column_headers(query, df=df_with_taxonomy)()
    filename = generate_filename_client2_supplemental()

    return GoogleDriveCsvFile.from_dataframe(
        destination=GoogleDriveDestination.XDRIVE_TMRP_MONTHLY,
        name=filename,
        data_frame=df_header_updated,
        file_extension="txt",
        sep="\t",
    )


@asset(group_name="reports_weekly_client2", io_manager_key="google_drive_io_manager")
def client2_statin_outreach_completed_visits_report():
    query = FileGenerationQueries.client2_STATIN_OUTREACH_COMPLETED_VISITS
    df = op_factory_from_query(query)()
    df_header_updated = update_column_headers(query, df=df)()
    filename = generate_file_name_client2_statin_outreach_completed_visits()

    return GoogleDriveCsvFile.from_dataframe(
        destination=GoogleDriveDestination.XDRIVE_STATIN_OUTREACH_WEEKLY,
        name=filename,
        data_frame=df_header_updated,
        file_extension="csv",
        sep=",",
    )


@asset(group_name="reports_weekly_client2", io_manager_key="google_drive_io_manager")
def client2_statin_outreach_member_disposition_report():
    query = FileGenerationQueries.client2_STATIN_OUTREACH_MEMBER_DISPOSITION
    df = op_factory_from_query(query)()
    df_header_updated = update_column_headers(query, df=df)()
    filename = generate_file_name_client2_statin_outreach_member_disposition()

    return GoogleDriveCsvFile.from_dataframe(
        destination=GoogleDriveDestination.XDRIVE_STATIN_OUTREACH_WEEKLY,
        name=filename,
        data_frame=df_header_updated,
        file_extension="csv",
        sep=",",
    )


client2_supplemental_file_job = define_asset_job(
    name="client2_supplemental_file_job", selection="client2_supplemental_file_report"
)

client2_statin_outreach_completed_visits_job = define_asset_job(
    name="client2_statin_outreach_completed_visits_job",
    selection="client2_statin_outreach_completed_visits_report",
)

client2_statin_outreach_member_disposition_job = define_asset_job(
    name="client2_statin_outreach_member_disposition_job",
    selection="client2_statin_outreach_member_disposition_report",
)


client2_sup_schedule = ScheduleDefinition(
    job=client2_supplemental_file_job,
    cron_schedule="0 14 * * 5#1",
    execution_timezone="US/Eastern",
    default_status=DefaultScheduleStatus.RUNNING,
)  # runs on the first Friday of every month at 2p EST

client2_statin_outreach_completed_visits_schedule = ScheduleDefinition(
    job=client2_statin_outreach_completed_visits_job,
    cron_schedule="0 15 * * 1",
    execution_timezone="US/Eastern",
    default_status=DefaultScheduleStatus.RUNNING,
)  # runs every Monday at 3pm EST

client2_statin_outreach_member_disposition_schedule = ScheduleDefinition(
    job=client2_statin_outreach_member_disposition_job,
    cron_schedule="0 15 * * 1",
    execution_timezone="US/Eastern",
    default_status=DefaultScheduleStatus.RUNNING,
)  # runs every Monday at 3pm EST
