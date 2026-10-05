from dagster import AssetKey, AssetSelection, define_asset_job

chirpstack_metrics_job = define_asset_job(
    name="023_sense_chirpstack_metrics",
    selection=AssetSelection.keys(
        AssetKey(["carlsjr", "observability_chirpstackmetrics"]),
        AssetKey(["carlsjr", "observability_chirpstackmqttmetrics"]),
    ),
    tags={"job_type": "observability", "run_lane": "observability", "user_code_run_lane": "carlsjr_observability", "user_code": "carlsjr"},
)
