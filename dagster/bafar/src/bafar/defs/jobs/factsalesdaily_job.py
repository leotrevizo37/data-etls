from dagster import AssetKey, AssetSelection, define_asset_job


fact_store_sales_daily_job = define_asset_job(
    name="021_bafar_store_sales_daily",
    selection=AssetSelection.keys(
        AssetKey(["bafar", "dwh_factstoresalesdaily"]),
    ),
    tags={"job_type": "ventas", "run_lane": "operational", "user_code_run_lane": "bafar_operational", "user_code": "bafar"},
)
