from dagster import AssetSelection, define_asset_job

bridge_cleanup_job = define_asset_job(
    name="bridge_cleanup_job",
    selection=AssetSelection.keys("bridge_cleanup_old_runs"),
    description="Runs the maintenance asset that prunes old Dagster bridge run history.",
    tags={"job_type": "limpieza_bridge"},
)
