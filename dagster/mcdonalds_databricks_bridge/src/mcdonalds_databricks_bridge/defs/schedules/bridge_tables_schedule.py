import os
from collections import defaultdict
from typing import Dict, Iterable, List, Optional

from dagster import DefaultScheduleStatus, ScheduleDefinition

from ..resources import BridgeTableConfig

_GROUP_CRON_DEFAULTS = {
    "dims": "0 11 * * 1",
    "facts": "0 11 * * 1-5",
}

_GROUP_CRON_ENV_VARS = {
    "dims": "BRIDGE_DIMS_SYNC_CRON",
    "facts": "BRIDGE_FACTS_SYNC_CRON",
}


def _is_truthy(value: str | None) -> bool:
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _resolve_job_group_token(job_name: str) -> str | None:
    if not job_name.startswith("bridge_") or not job_name.endswith("_job"):
        return None
    if job_name == "bridge_tables_job":
        return None
    return job_name[len("bridge_") : -len("_job")]


def _resolve_cron(job_group_token: str | None) -> str:
    if job_group_token:
        env_var = _GROUP_CRON_ENV_VARS.get(job_group_token)
        if env_var and os.getenv(env_var):
            return os.getenv(env_var, "")
        if job_group_token in _GROUP_CRON_DEFAULTS:
            return _GROUP_CRON_DEFAULTS[job_group_token]
    return os.getenv("BRIDGE_SYNC_CRON", "0 */8 * * *")


def _build_schedule_description(job_name: str, cron_schedule: str) -> str:
    job_group_token = _resolve_job_group_token(job_name)
    if job_group_token:
        return (
            "Schedules the `"
            + job_group_token
            + "` bridge ingestion job with cron `"
            + cron_schedule
            + "`."
        )
    return "Schedules the full bridge job with cron `" + cron_schedule + "`."


def build_bridge_schedules(bridge_jobs):
    schedules = []
    execution_timezone = os.getenv("BRIDGE_EXECUTION_TIMEZONE", "America/Ciudad_Juarez")
    include_all_tables_schedule = _is_truthy(os.getenv("BRIDGE_ENABLE_ALL_TABLES_SCHEDULE"))
    explicit_all_tables_cron = os.getenv("BRIDGE_ALL_TABLES_SYNC_CRON")

    for bridge_job in bridge_jobs:
        if bridge_job.name == "bridge_tables_job" and not (
            include_all_tables_schedule or explicit_all_tables_cron
        ):
            continue

        if bridge_job.name == "bridge_tables_job":
            cron_schedule = explicit_all_tables_cron or _resolve_cron(None)
        else:
            cron_schedule = _resolve_cron(_resolve_job_group_token(bridge_job.name))
        schedules.append(
            ScheduleDefinition(
                name=bridge_job.name.replace("_job", "_schedule"),
                job=bridge_job,
                cron_schedule=_resolve_schedule_cron(
                    bridge_job=bridge_job,
                    group_schedule_overrides=group_schedule_overrides,
                    default_cron=default_cron,
                    dims_cron=dims_cron,
                ),
                execution_timezone=execution_timezone,
                description=_build_schedule_description(bridge_job.name, cron_schedule),
                metadata={
                    "bridge_job_name": bridge_job.name,
                    "cron_schedule": cron_schedule,
                    "execution_timezone": execution_timezone,
                },
                default_status=DefaultScheduleStatus.STOPPED,
            )
        )

    return schedules
