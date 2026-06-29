from dagster import AssetKey, AssetSelection, define_asset_job, in_process_executor


sense_operational_job = define_asset_job(
    name="020_sense_operations_sensor_events",
    selection=AssetSelection.keys(
        AssetKey(["valledelencino", "dwh_factReadingsMeasurement"]),
        AssetKey(["valledelencino", "ctl_IngestionControl"]),
        AssetKey(["valledelencino", "dwh_factsensorevents"]),
    ),
    tags={"job_type": "operaciones", "user_code": "valledelencino"},
    executor_def=in_process_executor,
)
