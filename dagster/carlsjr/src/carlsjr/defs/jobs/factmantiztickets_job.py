from dagster import AssetKey, AssetSelection, define_asset_job, in_process_executor


fact_mantiz_tickets_job = define_asset_job(
    name="100_mantiz_tickets",
    selection=AssetSelection.keys(
        AssetKey(["carlsjr", "dwh_factmantizticketlifecycle"]),
        AssetKey(["carlsjr", "dwh_factmantiztickettextinsights"]),
    ),
    executor_def=in_process_executor,
    tags={"job_type": "mantiz", "user_code": "carlsjr"},
)
