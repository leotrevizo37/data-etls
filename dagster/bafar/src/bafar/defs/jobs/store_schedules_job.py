from dagster import AssetKey, AssetSelection, define_asset_job


store_schedules_job = define_asset_job(
    name="010_bafar_store_schedules",
    selection=AssetSelection.keys(
        AssetKey(["bafar", "dwh_dimstoreschedules"]),
    ),
    tags={
        "job_type": "api",
        "run_lane": "operational",
        "user_code_run_lane": "bafar_operational",
        "user_code": "bafar",
    },
)
