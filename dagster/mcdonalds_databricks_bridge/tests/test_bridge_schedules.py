from __future__ import annotations

import os

from mcdonalds_databricks_bridge.defs.jobs import build_bridge_jobs
from mcdonalds_databricks_bridge.defs.resources import load_table_configs
from mcdonalds_databricks_bridge.defs.schedules import build_bridge_schedules


def test_bridge_group_schedules_are_built_without_global_schedule(monkeypatch):
    monkeypatch.setenv("BRIDGE_SYNC_CRON", "0 */8 * * *")
    monkeypatch.setenv("BRIDGE_DIMS_SYNC_CRON", "0 11 * * 1")

    bridge_jobs = build_bridge_jobs(load_table_configs())
    schedules = build_bridge_schedules(bridge_jobs, load_table_configs())
    schedules_by_name = {schedule.name: schedule for schedule in schedules}

    assert "bridge_tables_schedule" not in schedules_by_name
    assert schedules_by_name["bridge_dims_schedule"].cron_schedule == "0 11 * * 1"
    assert schedules_by_name["bridge_facts_schedule"].cron_schedule == "0 */8 * * *"


def test_bridge_all_schedule_is_kept_when_only_one_group(monkeypatch):
    monkeypatch.setenv("BRIDGE_SYNC_CRON", "0 */8 * * *")
    monkeypatch.delenv("BRIDGE_DIMS_SYNC_CRON", raising=False)

    single_group_configs = [config for config in load_table_configs() if config.job_group == "facts"]
    bridge_jobs = build_bridge_jobs(single_group_configs)
    schedules = build_bridge_schedules(bridge_jobs, single_group_configs)

    assert len(schedules) == 1
    assert schedules[0].name == "bridge_tables_schedule"
    assert schedules[0].cron_schedule == os.environ["BRIDGE_SYNC_CRON"]
