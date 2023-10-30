import os

from dagster import Definitions, InMemoryIOManager, IOManager
from dagster_dbt import DbtCliResource
from dagster_slack import SlackResource

from cureatr.assets.dbt_assets import daily_dbt_assets_schedule, dw_dbt_assets
from cureatr.assets.test_files import (
    test_create_directory_tree,
    test_csv_file,
    test_dialer_split,
    test_text_file,
)
from cureatr.clients.bcbsm import (
    bcbsm_statin_outreach_completed_visits_job,
    bcbsm_statin_outreach_completed_visits_report,
    bcbsm_statin_outreach_completed_visits_schedule,
    bcbsm_statin_outreach_member_disposition_job,
    bcbsm_statin_outreach_member_disposition_report,
    bcbsm_statin_outreach_member_disposition_schedule,
    bcbsm_sup_schedule,
    bcbsm_supplemental_file_job,
    bcbsm_supplemental_file_report,
)
from cureatr.clients.humana import (
    humana_cahps_weekly_gnd_job,
    humana_cahps_weekly_gnd_report,
    humana_dmrp_supplemental_job,
    humana_dmrp_supplemental_monthly_schedule,
    humana_dmrp_supplemental_report,
    humana_dmrp_weekly_event_level_detail_report,
    humana_dmrp_weekly_event_level_job,
    humana_dmrp_weekly_map_extract_job,
    humana_dmrp_weekly_map_extract_job_txt,
    humana_dmrp_weekly_map_extract_report,
    humana_dmrp_weekly_map_extract_report_txt,
    humana_drmp_event_detail_weekly_schedule,
    humana_drmp_map_extract_weekly_schedule,
    humana_drmp_map_extract_weekly_txt_schedule,
    humana_gnd_weekly_outcomes_schedule,
)
from cureatr.constants import dbt_project_dir
from cureatr.infrastructure.dialer import (
    dialer_automation,
    dialer_automation_daily_schedule,
    dialer_automation_job,
)
from cureatr.io_managers.google_drive_io_manager import GoogleDriveIOManager
from cureatr.utils import google_credentials

from .jobs import hello_redshift_job


def configure_google_drive_io_manager() -> IOManager:
    credentials = google_credentials()
    if credentials:
        return GoogleDriveIOManager(auth_info=credentials)
    else:
        return InMemoryIOManager()


resources = {
    "google_drive_io_manager": configure_google_drive_io_manager(),
    "dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir)),
    "slack": SlackResource(token=os.getenv("SLACK_TOKEN")),
}

definitions = Definitions(
    assets=[
        test_create_directory_tree,
        test_csv_file,
        test_dialer_split,
        test_text_file,
        humana_cahps_weekly_gnd_report,
        humana_dmrp_weekly_event_level_detail_report,
        humana_dmrp_weekly_map_extract_report,
        humana_dmrp_weekly_map_extract_report_txt,
        humana_dmrp_supplemental_report,
        dw_dbt_assets,
        dialer_automation,
        bcbsm_supplemental_file_report,
        bcbsm_statin_outreach_completed_visits_report,
        bcbsm_statin_outreach_member_disposition_report,
    ],
    jobs=[
        hello_redshift_job,
        bcbsm_supplemental_file_job,
        bcbsm_statin_outreach_completed_visits_job,
        bcbsm_statin_outreach_member_disposition_job,
        dialer_automation_job,
        humana_cahps_weekly_gnd_job,
        humana_dmrp_weekly_event_level_job,
        humana_dmrp_weekly_map_extract_job,
        humana_dmrp_weekly_map_extract_job_txt,
        humana_dmrp_supplemental_job,
    ],
    resources=resources,
    schedules=[
        bcbsm_sup_schedule,
        bcbsm_statin_outreach_completed_visits_schedule,
        bcbsm_statin_outreach_member_disposition_schedule,
        dialer_automation_daily_schedule,
        humana_gnd_weekly_outcomes_schedule,
        daily_dbt_assets_schedule,
        humana_drmp_event_detail_weekly_schedule,
        humana_drmp_map_extract_weekly_schedule,
        humana_drmp_map_extract_weekly_txt_schedule,
        humana_dmrp_supplemental_monthly_schedule,
    ],
)
