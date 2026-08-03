from dagster import AssetKey, AssetSelection, define_asset_job


fact_operations_tables_job = define_asset_job(
    name="020_sense_operations",
    selection=AssetSelection.keys(
        AssetKey(["valledelencino", "data_transformations"]),
        AssetKey(["valledelencino", "dwh_factReadingsMeasurement"]),
        AssetKey(["valledelencino", "ctl_IngestionControl"])
    ),
    tags={"job_type": "operaciones", "run_lane": "operational", "user_code": "valledelencino"},
)
