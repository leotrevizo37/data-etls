from dagster import AssetKey, AssetSelection, define_asset_job


sidon_configuration_job = define_asset_job(
    name="sidon_configuration_job",
    selection=AssetSelection.assets(AssetKey(["valledelencino", "sidon_configuration"])),
    tags={"job_type": "alerting", "user_code_run_lane": "valledelencino_observability", "user_code": "valledelencino", "dagster/max_retries": "0"},
)
