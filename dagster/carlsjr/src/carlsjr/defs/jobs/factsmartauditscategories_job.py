from dagster import AssetKey, AssetSelection, define_asset_job


fact_smartaudits_categories_job = define_asset_job(
    name="110_smartaudits_categories",
    selection=AssetSelection.keys(
        AssetKey(["carlsjr", "dwh_factsmartauditscategories"]),
    ),
    tags={"job_type": "smartaudits", "run_lane": "operational", "user_code_run_lane": "carlsjr_operational", "user_code": "carlsjr"},
)
