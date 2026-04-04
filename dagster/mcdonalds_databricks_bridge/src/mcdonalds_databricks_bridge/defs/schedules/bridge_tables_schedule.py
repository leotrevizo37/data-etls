import os
from collections import defaultdict
from typing import Dict, Iterable, List, Optional

from dagster import DefaultScheduleStatus, ScheduleDefinition

from ..resources import BridgeTableConfig


def _resolve_group_schedule_overrides(
    table_configs: Iterable[BridgeTableConfig],
) -> Dict[str, str]:
    grouped_crons = defaultdict(set)

    for table_config in table_configs:
        if table_config.schedule_cron:
            grouped_crons[table_config.job_group].add(table_config.schedule_cron)

    resolved: Dict[str, str] = {}
    for group_name, cron_values in grouped_crons.items():
        if len(cron_values) != 1:
            raise ValueError(
                "Bridge table group '"
                + group_name
                + "' defines multiple schedule_cron values. Group jobs must share one cron."
            )

        resolved[group_name] = next(iter(cron_values))

    return resolved


def _resolve_schedule_cron(
    bridge_job,
    group_schedule_overrides: Dict[str, str],
    default_cron: str,
    dims_cron: str,
) -> str:
    bridge_group: Optional[str] = bridge_job.tags.get("bridge_group")
    bridge_scope: Optional[str] = bridge_job.tags.get("bridge_scope")

    if bridge_scope == "group" and bridge_group:
        if bridge_group in group_schedule_overrides:
            return group_schedule_overrides[bridge_group]
        if bridge_group == "dims":
            return dims_cron

    return default_cron


def _should_schedule_job(bridge_job, has_group_jobs: bool) -> bool:
    bridge_scope: Optional[str] = bridge_job.tags.get("bridge_scope")

    if not has_group_jobs:
        return True

    return bridge_scope == "group"


def build_bridge_schedules(bridge_jobs, table_configs):
    schedules: List[ScheduleDefinition] = []
    default_cron = os.getenv("BRIDGE_SYNC_CRON", "0 */8 * * *")
    dims_cron = os.getenv("BRIDGE_DIMS_SYNC_CRON", "0 11 * * 1")
    execution_timezone = os.getenv("BRIDGE_EXECUTION_TIMEZONE", "America/Ciudad_Juarez")
    group_schedule_overrides = _resolve_group_schedule_overrides(table_configs)
    has_group_jobs = any(bridge_job.tags.get("bridge_scope") == "group" for bridge_job in bridge_jobs)

    for bridge_job in bridge_jobs:
        if not _should_schedule_job(bridge_job, has_group_jobs=has_group_jobs):
            continue

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
                default_status=DefaultScheduleStatus.STOPPED,
            )
        )

    return schedules
