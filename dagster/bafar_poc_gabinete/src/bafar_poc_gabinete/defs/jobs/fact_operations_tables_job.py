from dagster import AssetKey, AssetSelection, define_asset_job


fact_operations_tables_job = define_asset_job(
    name="020_sense_operations",
    selection=AssetSelection.keys(
        AssetKey(["bafar_poc_gabinete", "data_transformations"]),
        AssetKey(["bafar_poc_gabinete", "dwh_factReadingsMeasurement"]),
        AssetKey(["bafar_poc_gabinete", "ctl_IngestionControl"])
    ),
    tags={"job_type": "operaciones", "run_lane": "operational", "user_code_run_lane": "bafar_poc_gabinete_operational", "user_code": "bafar_poc_gabinete"},
)
