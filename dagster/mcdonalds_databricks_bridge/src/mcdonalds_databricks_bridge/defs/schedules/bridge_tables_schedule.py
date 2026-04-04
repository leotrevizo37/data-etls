import os
from collections import defaultdict
from typing import Dict, Iterable, List, Optional

from dagster import DefaultScheduleStatus, ScheduleDefinition

from ..resources import BridgeTableConfig

def build_bridge_schedules(bridge_jobs):
    schedules = []
    cron_schedule = os.getenv("BRIDGE_SYNC_CRON", "0 */8 * * *")
    execution_timezone = os.getenv("BRIDGE_EXECUTION_TIMEZONE", "America/Ciudad_Juarez")

    for bridge_job in bridge_jobs:
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
