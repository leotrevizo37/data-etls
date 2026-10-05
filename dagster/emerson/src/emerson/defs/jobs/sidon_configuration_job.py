from dagster import AssetKey, AssetSelection, define_asset_job


sidon_configuration_job = define_asset_job(
    name="sidon_configuration_job",
    selection=AssetSelection.assets(AssetKey(["emerson", "sidon_configuration"])),
    tags={"job_type": "alerting", "user_code_run_lane": "emerson_observability", "user_code": "emerson", "dagster/max_retries": "0"},
)
