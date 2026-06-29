from dagster import AssetKey, AssetSelection, define_asset_job, in_process_executor


sense_operational_job = define_asset_job(
    name="020_sense_operations_sensor_events",
    selection=AssetSelection.keys(
        AssetKey(["carlsjr", "dwh_factReadingsMeasurement"]),
        AssetKey(["carlsjr", "ctl_IngestionControl"]),
        AssetKey(["carlsjr", "dwh_factsensorevents"]),
    ),
    tags={"job_type": "operaciones", "user_code": "carlsjr"},
    executor_def=in_process_executor,
)
