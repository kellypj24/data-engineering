import pendulum
from dagster import DefaultScheduleStatus, ScheduleDefinition, asset, define_asset_job

from cureatr.io_managers.google_drive_io_manager import (
    FileNameConstructor,
    GoogleDriveCsvFile,
    GoogleDriveDestination,
)
from cureatr.ops.file_generation_ops import (
    op_factory_from_query,
)
from cureatr.sql import FileGenerationQueries


@asset(group_name="dialer", io_manager_key="google_drive_io_manager")
def dialer_automation():
    query = FileGenerationQueries.DIALER_LOADER_CMR_COMMERCIAL
    df = op_factory_from_query(query)()

    # Define custom filename constructor
    filename_constructor = FileNameConstructor(query.filename_pattern)

    # Convert single DataFrame to list as input
    google_drive_files = GoogleDriveCsvFile.from_dataframes(
        destination=GoogleDriveDestination.PHI_DATA_TEAM_REPORTING,
        data_frames=[df],
        columns_to_group="campaign",
        directory_path=f"dialer_loader_{pendulum.now().format('YYYYMMDD')}",
        filename_constructor=filename_constructor,
    )

    return google_drive_files  # Returning list of GoogleDriveCSVFile instances directly


dialer_automation_job = define_asset_job(name="dialer_automation_job", selection="dialer_automation")

dialer_automation_daily_schedule = ScheduleDefinition(
    job=dialer_automation_job,
    cron_schedule="0 5 * * *",
    execution_timezone="US/Eastern",
    default_status=DefaultScheduleStatus.RUNNING,
)  # runs everday at 5a EST
