from dagster import AssetKey, AssetSelection, RetryPolicy, define_asset_job

SENSOR_EXPECTED_VALUES_RETRY_POLICY = RetryPolicy(max_retries=3, delay=60)

refresh_sensor_expected_values_job = define_asset_job(
    name="refresh_sensor_expected_values_job",
    selection=AssetSelection.keys(AssetKey(["smartfit", "dwh_dimsensorexpectedval"])),
    tags={"job_type": "dimensiones", "user_code": "smartfit"},
    op_retry_policy=SENSOR_EXPECTED_VALUES_RETRY_POLICY,
)
