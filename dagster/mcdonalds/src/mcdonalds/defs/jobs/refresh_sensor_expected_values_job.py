from dagster import AssetSelection, define_asset_job


refresh_sensor_expected_values_job = define_asset_job(
    name="refresh_sensor_expected_values_job",
    selection=AssetSelection.keys("dwh_dimsensorexpectedval"),
    tags={"job_type": "dimensiones"},
)
