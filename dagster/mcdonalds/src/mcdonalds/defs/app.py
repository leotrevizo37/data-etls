from dagster import Definitions

from .assets import (dbo_sensors,
                     dbo_sensor_tys,
                     dbo_devices,
                     dbo_device_tys,
                     dbo_sublocations,
                     dwh_factlecturas,
                     operations_assets,
                     dwh_dimsidonproddimensions)
from .schedules import (fact_lecturas_sched_every_2h,
                        operations_sched_every_4h)
from .jobs import (fact_lecturas_ingestion_job,
                   fact_operations_tables_job,
                   refresh_dimensions_job)
from .sensors import dwh_dimsidonproddimensions_sensor

defs = Definitions(
    jobs=[
        fact_lecturas_ingestion_job,
        fact_operations_tables_job,
        refresh_dimensions_job
    ],
    schedules=[
        fact_lecturas_sched_every_2h,
        operations_sched_every_4h,
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
        dwh_dimsidonproddimensions
    ],
)
