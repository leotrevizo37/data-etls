from dagster import AssetKey, AssetSelection, define_asset_job


operational_alerts_job = define_asset_job(
    name="operational_alerts_job",
    selection=AssetSelection.assets(AssetKey(["bafar", "operational_alerts"])),
    tags={"job_type": "alerting", "user_code_run_lane": "bafar_observability", "user_code": "bafar", "dagster/max_retries": "0"},
)
