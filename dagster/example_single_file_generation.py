from dagster import (
    DefaultScheduleStatus,
    ScheduleDefinition,
    asset,
    define_asset_job,
)

from home.io_managers.google_drive_io_manager import (
    GoogleDriveCSVFile,
    GoogleDriveDestination,
)
from home.ops.file_generation_ops import (
    op_factory_from_query,
    update_column_headers,
)
from home.sql import FileGenerationQueries


@asset(group_name="reports_weekly_report", io_manager_key="google_drive_io_manager")
def bcbsm_statin_outreach_completed_visits_report():
    query = FileGenerationQueries.WEEKLY_REPORT_EXAMPLE
    df = op_factory_from_query(query)()
    df_header_updated = update_column_headers(query, df=df)()
    filename = query.filename_pattern

    return GoogleDriveCSVFile.from_dataframe(
        destination=GoogleDriveDestination.DESTINATION_DRIVE,
        name=filename,
        data_frame=df_header_updated,
        file_extension="csv",
        sep=",",
    )


weekly_file_job = define_asset_job(
    name="weekly_file_job", selection="weekly_file_report"
)


weekly_file_schedule = ScheduleDefinition(
    job=weekly_file_job,
    cron_schedule="0 15 * * 1",
    execution_timezone="US/Eastern",
    default_status=DefaultScheduleStatus.RUNNING,
)  # runs every Monday at 3pm EST
