from dagster import AssetKey, AssetSelection, define_asset_job


manager_access_daily_job = define_asset_job(
    name="024_bafar_manager_access_daily",
    selection=AssetSelection.keys(
        AssetKey(["bafar", "dwh_factmanageraccessdaily"]),
    ),
    tags={
        "job_type": "api",
        "run_lane": "operational",
        "user_code_run_lane": "bafar_operational",
        "user_code": "bafar",
    },
)
