from dagster import Definitions

from .assets import (dbo_sensors,
                     dwh_factHourlyDeviceStatus,
                     dbo_sensor_tys,
                     dbo_devices,
                     dbo_device_tys,
                     dbo_sublocations,
                     dwh_factlecturas)
from .schedules import (fact_lecturas_sched_every_2h,
                        operations_sched_every_4h)
from .jobs import (fact_lecturas_ingestion_job,
                   facthourlydevicestatus_ingestion)

defs = Definitions(
    jobs=[
        fact_lecturas_ingestion_job,
        facthourlydevicestatus_ingestion
    ],
    schedules=[
        fact_lecturas_sched_every_2h,
        operations_sched_every_4h,
    ],
    assets=[
        dwh_factHourlyDeviceStatus,
        dbo_sensors,
        dbo_sensor_tys,
        dbo_devices,
        dbo_device_tys,
        dbo_sublocations,
        dwh_factlecturas
    ],
)
