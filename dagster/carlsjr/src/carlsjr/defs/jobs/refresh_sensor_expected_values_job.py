from dagster import AssetKey, AssetSelection, RetryPolicy, define_asset_job

SENSOR_EXPECTED_VALUES_RETRY_POLICY = RetryPolicy(max_retries=3, delay=60)

refresh_sensor_expected_values_job = define_asset_job(
    name="083_maintenance_refresh_sensor_expected_values",
    selection=AssetSelection.keys(AssetKey(["carlsjr", "dwh_dimsensorexpectedval"])),
    tags={"job_type": "sensor_expected_values", "run_lane": "operational", "user_code_run_lane": "carlsjr_operational", "user_code": "carlsjr"},
    op_retry_policy=SENSOR_EXPECTED_VALUES_RETRY_POLICY,
)
