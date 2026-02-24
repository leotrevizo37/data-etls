from dagster import Definitions

from .jobs import (fact_operations_tables_job,
                   fact_lecturas_ingestion_job,
                   refresh_dimensions_job,
                   cleanup_job)
from .resources import (dim_sqlserver_resource,
                        spark_resource,
                        sqlserver_resource,
                        transformation_ops,
                        spark_df_io_manager)
from .assets import (dbo_sensors,
                     dbo_sensor_tys,
                     dbo_devices,
                     dbo_device_tys,
                     dbo_sublocations,
                     dwh_factlecturas,
                     operations_assets,
                     dwh_dimsidonproddimensions,
                     cleanup_old_runs)
from .schedules import (fact_lecturas_sched_every_2h,
                        fact_lecturas_sched_every_w,
                        fact_lecturas_sched_every_1h,
                        operations_sched_every_4h,
                        operations_sched_every_w,
                        operations_sched_every_2h,
                        cleanup_schedule )
from .sensors import dwh_dimsidonproddimensions_sensor


defs = Definitions(
    jobs=[
        fact_lecturas_ingestion_job,
        fact_operations_tables_job,
        refresh_dimensions_job,
        cleanup_job
    ],
    resources={
        "sqlserver": sqlserver_resource,
        "spark": spark_resource,
        "dim_sqlserver": dim_sqlserver_resource,
        "transformation_ops": transformation_ops,
        "spark_df_io_manager": spark_df_io_manager,
    },
    schedules=[
        fact_lecturas_sched_every_2h,
        fact_lecturas_sched_every_w,
        fact_lecturas_sched_every_1h,
        operations_sched_every_4h,
        operations_sched_every_w,
        operations_sched_every_2h,
        cleanup_schedule
    ],
    sensors=[
        dwh_dimsidonproddimensions_sensor
    ],
    assets=[
        dbo_sensors,
        dbo_sensor_tys,
        dbo_devices,
        dbo_device_tys,
        dbo_sublocations,
        dwh_factlecturas,
        operations_assets,
        dwh_dimsidonproddimensions,
        cleanup_old_runs
    ],
)
