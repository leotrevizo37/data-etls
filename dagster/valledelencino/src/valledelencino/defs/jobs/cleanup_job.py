from dagster import AssetKey, AssetSelection, define_asset_job


cleanup_job = define_asset_job(
    name="cleanup_job",
    selection=AssetSelection.keys(
        AssetKey(["valledelencino", "cleanup_old_runs"]),
    ),
    tags={"job_type": "limpieza", "user_code": "valledelencino"},
)
