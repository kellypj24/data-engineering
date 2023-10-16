import pendulum
from dagster import DefaultScheduleStatus, ScheduleDefinition, asset, define_asset_job

from home.io_managers.google_drive_io_manager import (
    FileNameConstructor,
    GoogleDriveCSVFile,
    GoogleDriveDestination,
)
from home.ops.file_generation_ops import (
    op_factory_from_query,
)
from home.sql import FileGenerationQueries


@asset(group_name="multi_file_group", io_manager_key="google_drive_io_manager")
def dialer_automation():
    query = FileGenerationQueries.EXAMPLE_QUERY
    df = op_factory_from_query(query)()

    # Define custom filename constructor
    filename_constructor = FileNameConstructor(query.filename_pattern)

    # Convert single DataFrame to list as input
    google_drive_files = GoogleDriveCSVFile.from_dataframes(
        destination=GoogleDriveDestination.DESTINATION_GOOGLE_DRIVE,
        data_frames=[df],
        columns_to_group="group_name",
        folder_path=f"example_folder_{pendulum.now().format('YYYYMMDD')}",
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
