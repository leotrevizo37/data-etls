from dagster import AssetKey, AssetSelection, RetryPolicy, define_asset_job

DIMENSION_REFRESH_RETRY_POLICY = RetryPolicy(max_retries=3, delay=60)

refresh_dimensions_job = define_asset_job(
    name="082_maintenance_refresh_dimensions",
    selection=AssetSelection.keys(AssetKey(["bafar_poc_gabinete", "dwh_dimsidonproddimensions"])),
    tags={"job_type": "dimensiones", "run_lane": "operational", "user_code_run_lane": "bafar_poc_gabinete_operational", "user_code": "bafar_poc_gabinete"},
    op_retry_policy=DIMENSION_REFRESH_RETRY_POLICY,
)
