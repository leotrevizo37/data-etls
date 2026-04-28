from dagster import AssetKey, AssetSelection, define_asset_job

fact_operations_tables_job = define_asset_job(
    name="fact_operations_tables_job",
    selection=AssetSelection.keys(
        AssetKey(["smartfit", "data_transformations"]),
        AssetKey(["smartfit", "dwh_factHourlyDeviceStatus"]),
        AssetKey(["smartfit", "dwh_factReadingsMeasurement"]),
        AssetKey(["smartfit", "ctl_IngestionControl"])
    ),
    tags={"job_type": "operaciones", "user_code": "smartfit"},
)
