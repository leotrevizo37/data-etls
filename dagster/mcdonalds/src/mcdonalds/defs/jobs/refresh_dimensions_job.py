from dagster import AssetKey, AssetSelection, RetryPolicy, define_asset_job

DIMENSION_REFRESH_RETRY_POLICY = RetryPolicy(max_retries=3, delay=60)

refresh_dimensions_job = define_asset_job(
    name="refresh_dimensions_job",
    selection=AssetSelection.keys(AssetKey(["mcdonalds", "dwh_dimsidonproddimensions"])),
    tags={"job_type": "dimensiones", "user_code": "mcdonalds"},
    op_retry_policy=DIMENSION_REFRESH_RETRY_POLICY,
)
