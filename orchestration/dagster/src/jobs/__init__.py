"""Job definitions — re-exported from submodules."""

from src.jobs.landing import process_landing_file_job

all_jobs = [
    process_landing_file_job,
]
