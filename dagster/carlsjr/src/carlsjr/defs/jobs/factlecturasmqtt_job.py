from dagster import AssetKey, AssetSelection, define_asset_job


factlecturasmqtt_job = define_asset_job(
    name="023_observability_factlecturasmqtt",
    selection=AssetSelection.keys(
        AssetKey(["carlsjr", "observability_factlecturasmqtt"]),
    ),
    tags={
        "job_type": "observability",
        "run_lane": "observability",
        "user_code_run_lane": "carlsjr_observability",
        "api_rate_limit": "carlsjr_lecturas_tenant",
        "user_code": "carlsjr",
    },
)
