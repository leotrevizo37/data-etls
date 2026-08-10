from dagster import AssetKey, AssetSelection, define_asset_job


cleanup_job = define_asset_job(
    name="080_maintenance_cleanup",
    selection=AssetSelection.keys(
        AssetKey(["bafar_poc_gabinete", "cleanup_old_runs"]),
    ),
    tags={"job_type": "limpieza", "run_lane": "operational", "user_code_run_lane": "bafar_poc_gabinete_operational", "user_code": "bafar_poc_gabinete"},
)
