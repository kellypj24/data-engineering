import os

from dagster import Definitions, InMemoryIOManager, IOManager
from dagster_dbt import DbtCliResource
from dagster_slack import SlackResource

from company.assets.dbt_assets import daily_dbt_assets_schedule, dw_dbt_assets
from company.assets.test_files import (
    test_create_directory_tree,
    test_csv_file,
    test_dialer_split,
    test_text_file,
)
from company.clients.client2 import (
    client2_statin_outreach_completed_visits_job,
    client2_statin_outreach_completed_visits_report,
    client2_statin_outreach_completed_visits_schedule,
    client2_statin_outreach_member_disposition_job,
    client2_statin_outreach_member_disposition_report,
    client2_statin_outreach_member_disposition_schedule,
    client2_sup_schedule,
    client2_supplemental_file_job,
    client2_supplemental_file_report,
)
from company.clients.client3 import (
    client3_cahps_weekly_gnd_job,
    client3_cahps_weekly_gnd_report,
    client3_dmrp_supplemental_job,
    client3_dmrp_supplemental_monthly_schedule,
    client3_dmrp_supplemental_report,
    client3_dmrp_weekly_event_level_detail_report,
    client3_dmrp_weekly_event_level_job,
    client3_dmrp_weekly_map_extract_job,
    client3_dmrp_weekly_map_extract_job_txt,
    client3_dmrp_weekly_map_extract_report,
    client3_dmrp_weekly_map_extract_report_txt,
    client3_drmp_event_detail_weekly_schedule,
    client3_drmp_map_extract_weekly_schedule,
    client3_drmp_map_extract_weekly_txt_schedule,
    client3_gnd_weekly_outcomes_schedule,
)
from company.constants import dbt_project_dir
from company.infrastructure.dialer import (
    dialer_automation,
    dialer_automation_daily_schedule,
    dialer_automation_job,
)
from company.io_managers.google_drive_io_manager import GoogleDriveIOManager
from company.utils import google_credentials

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
        client3_cahps_weekly_gnd_report,
        client3_dmrp_weekly_event_level_detail_report,
        client3_dmrp_weekly_map_extract_report,
        client3_dmrp_weekly_map_extract_report_txt,
        client3_dmrp_supplemental_report,
        dw_dbt_assets,
        dialer_automation,
        client2_supplemental_file_report,
        client2_statin_outreach_completed_visits_report,
        client2_statin_outreach_member_disposition_report,
    ],
    jobs=[
        hello_redshift_job,
        client2_supplemental_file_job,
        client2_statin_outreach_completed_visits_job,
        client2_statin_outreach_member_disposition_job,
        dialer_automation_job,
        client3_cahps_weekly_gnd_job,
        client3_dmrp_weekly_event_level_job,
        client3_dmrp_weekly_map_extract_job,
        client3_dmrp_weekly_map_extract_job_txt,
        client3_dmrp_supplemental_job,
    ],
    resources=resources,
    schedules=[
        client2_sup_schedule,
        client2_statin_outreach_completed_visits_schedule,
        client2_statin_outreach_member_disposition_schedule,
        dialer_automation_daily_schedule,
        client3_gnd_weekly_outcomes_schedule,
        daily_dbt_assets_schedule,
        client3_drmp_event_detail_weekly_schedule,
        client3_drmp_map_extract_weekly_schedule,
        client3_drmp_map_extract_weekly_txt_schedule,
        client3_dmrp_supplemental_monthly_schedule,
    ],
)
