from dagster import AssetKey, AssetSelection, define_asset_job

chirpstack_metrics_job = define_asset_job(
    name="023_sense_chirpstack_metrics",
    selection=AssetSelection.keys(
        AssetKey(["emerson", "observability_chirpstackmetrics"]),
    ),
    tags={"job_type": "observability", "run_lane": "observability", "user_code_run_lane": "emerson_observability", "user_code": "emerson"},
)
