from dagster import AssetKey, AssetSelection, define_asset_job


sidon_configuration_job = define_asset_job(
    name="sidon_configuration_job",
    selection=AssetSelection.assets(AssetKey(["ecosat", "sidon_configuration"])),
    tags={"job_type": "alerting", "user_code_run_lane": "ecosat_observability", "user_code": "ecosat", "dagster/max_retries": "0"},
)
