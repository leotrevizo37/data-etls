from dagster import AssetKey, AssetSelection, define_asset_job

fact_operations_tables_job = define_asset_job(
    name="020_sense_operations",
    selection=AssetSelection.keys(
        AssetKey(["mcdonalds_cdp", "data_transformations"]),
        AssetKey(["mcdonalds_cdp", "dwh_factReadingsMeasurement"]),
        AssetKey(["mcdonalds_cdp", "ctl_IngestionControl"])
    ),
    tags={"job_type": "operaciones", "run_lane": "operational", "user_code": "mcdonalds_cdp"},
)
