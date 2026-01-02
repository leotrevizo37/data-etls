from dagster import Definitions

from .jobs import (facthourlydevicestatus_ingestion,
                   fact_lecturas_ingestion_job)
from .resources import (dim_sqlserver_resource,
                       spark_resource,
                       sqlserver_resource,
                       transformation_ops)
from .assets import (dbo_sensors,
                     dwh_factHourlyDeviceStatus,
                     dbo_sensor_tys,
                     dbo_devices,
                     dbo_device_tys,
                     dbo_sublocations,
                     dwh_factlecturas)
from .schedules import (fact_lecturas_sched_every_2h,
                        operations_sched_every_4h )

defs = Definitions(
    jobs=[
        fact_lecturas_ingestion_job,
        facthourlydevicestatus_ingestion
    ],
    resources={
        "sqlserver": sqlserver_resource,
        "spark": spark_resource,
        "dim_sqlserver": dim_sqlserver_resource,
        "transformation_ops": transformation_ops,
    },
    schedules=[
        fact_lecturas_sched_every_2h,
        operations_sched_every_4h
    ],
    assets=[
        dbo_sensors,
        dbo_sensor_tys,
        dbo_devices,
        dbo_device_tys,
        dbo_sublocations,
        dwh_factHourlyDeviceStatus,
        dwh_factlecturas
    ],
)
