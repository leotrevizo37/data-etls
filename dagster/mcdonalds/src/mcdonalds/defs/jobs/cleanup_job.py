from dagster import define_asset_job, AssetSelection

cleanup_job = define_asset_job(
    name="cleanup_job",
    selection=AssetSelection.keys(
        "cleanup_old_runs",
    ),
    tags={"job_type": "limpieza", "user_code": "mcdonalds"},
)