from dagster import Definitions, definitions

from .defs.assets import (
    build_bridge_assets,
    build_source_assets,
    bridge_cleanup_old_runs,
)
from .defs.jobs import bridge_cleanup_job, build_bridge_jobs
from .defs.resources import (
    bridge_state_sqlserver_resource,
    databricks_sql_resource,
    load_table_configs,
    source_mysql_resource,
    source_sqlserver_resource,
)
from .defs.schedules import bridge_cleanup_schedule, build_bridge_schedules


@definitions
def defs():
    table_configs = load_table_configs()
    source_assets = build_source_assets(table_configs)
    bridge_assets = build_bridge_assets(table_configs)
    bridge_jobs = build_bridge_jobs(table_configs)
    bridge_schedules = build_bridge_schedules(bridge_jobs)

    return Definitions(
        assets=[*source_assets, *bridge_assets, bridge_cleanup_old_runs],
        jobs=[*bridge_jobs, bridge_cleanup_job],
        schedules=[*bridge_schedules, bridge_cleanup_schedule],
        resources={
            "bridge_state_sqlserver": bridge_state_sqlserver_resource,
            "databricks_sql": databricks_sql_resource,
            "source_mysql": source_mysql_resource,
            "source_sqlserver": source_sqlserver_resource,
        },
    )
