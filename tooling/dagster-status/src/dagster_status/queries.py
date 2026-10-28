"""Every GraphQL document this tool sends. Queries only: ``client.execute``
refuses anything else, and tests/test_queries.py fails if one of these is not
a query."""

RUN_FIELDS = "runId jobName status startTime endTime"

RUNS = f"""
query Runs($limit: Int!, $statuses: [RunStatus!]) {{
  runsOrError(limit: $limit, filter: {{statuses: $statuses}}) {{
    __typename
    ... on Runs {{ results {{ {RUN_FIELDS} }} }}
    ... on PythonError {{ message }}
  }}
}}
"""

DAEMON_HEALTH = """
query DaemonHealth {
  instance {
    daemonHealth {
      allDaemonStatuses { daemonType required healthy }
    }
  }
}
"""

# Schedules and sensors of every code location, with their live state.
INSTIGATORS = """
query Instigators {
  repositoriesOrError {
    __typename
    ... on RepositoryConnection {
      nodes {
        name
        location { name }
        schedules {
          name
          cronSchedule
          executionTimezone
          pipelineName
          scheduleState { status }
        }
        sensors {
          name
          sensorType
          targets { pipelineName }
          sensorState { status }
        }
      }
    }
    ... on PythonError { message }
  }
}
"""

ALL = {"RUNS": RUNS, "DAEMON_HEALTH": DAEMON_HEALTH, "INSTIGATORS": INSTIGATORS}
