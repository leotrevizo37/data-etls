from dagster import AssetKey, AssetSelection, RetryPolicy, define_asset_job

DIMENSION_REFRESH_RETRY_POLICY = RetryPolicy(max_retries=3, delay=60)

refresh_dimensions_job = define_asset_job(
    name="082_maintenance_refresh_dimensions",
    selection=AssetSelection.keys(AssetKey(["mcdonalds_cdp", "dwh_dimsidonproddimensions"])),
    tags={"job_type": "dimensiones", "user_code": "mcdonalds_cdp"},
    op_retry_policy=DIMENSION_REFRESH_RETRY_POLICY,
)
