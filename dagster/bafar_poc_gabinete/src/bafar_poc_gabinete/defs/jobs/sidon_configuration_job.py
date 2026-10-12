from dagster import AssetKey, AssetSelection, define_asset_job


sidon_configuration_job = define_asset_job(
    name="sidon_configuration_job",
    selection=AssetSelection.assets(AssetKey(["bafar_poc_gabinete", "sidon_configuration"])),
    tags={"job_type": "alerting", "user_code_run_lane": "bafar_poc_gabinete_observability", "user_code": "bafar_poc_gabinete", "dagster/max_retries": "0"},
)
