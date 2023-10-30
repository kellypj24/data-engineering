import json

from dagster import OpExecutionContext
from dagster_dbt import DbtCliResource, build_schedule_from_dbt_selection, dbt_assets
from dagster_slack import SlackResource

from company.constants import dbt_manifest_path


@dbt_assets(manifest=dbt_manifest_path)
def dw_dbt_assets(context: OpExecutionContext, dbt: DbtCliResource, slack: SlackResource):
    yield from dbt.cli(["seed"], context=context).stream()
    yield from dbt.cli(["run"], context=context).stream()

    test_invocation = dbt.cli(["test"], raise_on_error=False, context=context)
    yield from test_invocation.stream()

    # Extract run_results.json
    run_results_path = test_invocation.target_path.joinpath("run_results.json")
    with open(run_results_path) as json_file:
        run_results_json = json.load(json_file)

        # Get the list of results
        results = run_results_json.get("results", [])

        # Total tests run is simply the length of the results list
        total_tests_run = len(results)

        # Filter for failing tests and extract the desired fields
        failed_tests = [
            {
                "test_name": result["unique_id"].split("test.dw.")[1].split(".")[0],  # Extracting test name
                "message": result["message"].split(",")[0] if result["message"] else "No message",
                # Extracting "Got X results" part
            }
            for result in results
            if result.get("status") == "fail"
        ]

        # Send failing tests to Slack
        slack_client = slack.get_client()
        failures = [f":exclamation: {test['test_name']} - {test['message']}" for test in failed_tests]

        slack_message = ""
        if failures:
            slack_message = (
                "DBT Tests Failed in last run:\n"
                + f"{len(failed_tests)} of {total_tests_run} dbt tests failed in last run:\n"
                + "\n".join(failures)
            )
        else:
            slack_message = f"All {total_tests_run} dbt tests passed in last run."

        slack_client.chat_postMessage(channel="#data-alerts", text=slack_message)


daily_dbt_assets_schedule = build_schedule_from_dbt_selection(
    [dw_dbt_assets],
    job_name="daily_dbt_models",
    cron_schedule="0 11 * * *",
)
