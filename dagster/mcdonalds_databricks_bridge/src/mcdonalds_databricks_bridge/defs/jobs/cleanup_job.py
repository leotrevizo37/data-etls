from dagster import AssetSelection, define_asset_job

bridge_cleanup_job = define_asset_job(
    name="bridge_cleanup_job",
    selection=AssetSelection.keys("bridge_cleanup_old_runs"),
    description="Runs the bridge maintenance asset that deletes old Dagster run records.",
    tags={"job_type": "limpieza_bridge"},
)
