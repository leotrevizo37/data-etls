from dagster import AssetKey, AssetSelection, define_asset_job


operational_alerts_job = define_asset_job(
    name="operational_alerts_job",
    selection=AssetSelection.assets(AssetKey(["bafar_poc_gabinete", "operational_alerts"])),
    tags={"job_type": "alerting", "user_code_run_lane": "bafar_poc_gabinete_observability", "user_code": "bafar_poc_gabinete", "dagster/max_retries": "0"},
)
