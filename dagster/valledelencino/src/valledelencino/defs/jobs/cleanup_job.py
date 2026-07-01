from dagster import AssetKey, AssetSelection, define_asset_job


cleanup_job = define_asset_job(
    name="080_maintenance_cleanup",
    selection=AssetSelection.keys(
        AssetKey(["valledelencino", "cleanup_old_runs"]),
    ),
    tags={"job_type": "limpieza", "user_code": "valledelencino"},
)
