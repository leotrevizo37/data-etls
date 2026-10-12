from dagster import AssetKey, AssetSelection, define_asset_job


fact_operations_tables_job = define_asset_job(
    name="020_sense_operations",
    selection=AssetSelection.keys(
        AssetKey(["ecosat", "data_transformations"]),
        AssetKey(["ecosat", "dwh_factReadingsMeasurement"]),
        AssetKey(["ecosat", "ctl_IngestionControl"])
    ),
    tags={"job_type": "operaciones", "run_lane": "operational", "user_code_run_lane": "ecosat_operational", "user_code": "ecosat"},
)
