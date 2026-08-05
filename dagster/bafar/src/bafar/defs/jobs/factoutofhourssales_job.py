from dagster import AssetKey, AssetSelection, define_asset_job


fact_out_of_hours_sales_job = define_asset_job(
    name="020_bafar_out_of_hours_sales",
    selection=AssetSelection.keys(
        AssetKey(["bafar", "dwh_factoutofhourssales"]),
    ),
    tags={"job_type": "ventas", "run_lane": "operational", "user_code_run_lane": "bafar_operational", "user_code": "bafar"},
)
