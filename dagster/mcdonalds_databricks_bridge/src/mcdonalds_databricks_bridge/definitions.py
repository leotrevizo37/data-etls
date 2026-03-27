from dagster import Definitions, definitions

from .defs.assets import (
    bridge_cleanup_old_runs,
    dbx_tablaA_bridge,
    dbx_tablaB_bridge,
    dbx_tablaC_bridge,
    src_tablaA,
    src_tablaB,
    src_tablaC,
)
from .defs.jobs import bridge_cleanup_job, bridge_tables_job
from .defs.resources import (
    bridge_state_sqlserver_resource,
    databricks_sql_resource,
    source_sqlserver_resource,
)
from .defs.schedules import bridge_cleanup_schedule, bridge_tables_schedule


@definitions
def defs():
    return Definitions(
        assets=[
            src_tablaA,
            src_tablaB,
            src_tablaC,
            dbx_tablaA_bridge,
            dbx_tablaB_bridge,
            dbx_tablaC_bridge,
            bridge_cleanup_old_runs,
        ],
        jobs=[bridge_tables_job, bridge_cleanup_job],
        schedules=[bridge_tables_schedule, bridge_cleanup_schedule],
        resources={
            "bridge_state_sqlserver": bridge_state_sqlserver_resource,
            "databricks_sql": databricks_sql_resource,
            "source_sqlserver": source_sqlserver_resource,
        },
    )
