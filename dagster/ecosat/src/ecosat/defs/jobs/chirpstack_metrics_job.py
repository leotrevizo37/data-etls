from dagster import AssetKey, AssetSelection, define_asset_job

chirpstack_metrics_job = define_asset_job(
    name="023_sense_chirpstack_metrics",
    selection=AssetSelection.keys(
        AssetKey(["ecosat", "observability_chirpstackmetrics"]),
        AssetKey(["ecosat", "observability_chirpstackmqttmetrics"]),
    ),
    tags={"job_type": "observability", "run_lane": "observability", "user_code_run_lane": "ecosat_observability", "user_code": "ecosat"},
)
