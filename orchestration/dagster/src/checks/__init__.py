"""Asset check definitions — re-exported from submodules."""

from src.checks.freshness import freshness_check

all_checks = [
    freshness_check,
]
