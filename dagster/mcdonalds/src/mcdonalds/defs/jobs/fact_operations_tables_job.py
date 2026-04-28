from dagster import AssetKey, AssetSelection, define_asset_job

fact_operations_tables_job = define_asset_job(
    name="fact_operations_tables_job",
    selection=AssetSelection.keys(
        AssetKey(["mcdonalds", "data_transformations"]),
        AssetKey(["mcdonalds", "dwh_factHourlyDeviceStatus"]),
        AssetKey(["mcdonalds", "dwh_factReadingsMeasurement"]),
        AssetKey(["mcdonalds", "ctl_IngestionControl"])
    ),
    tags={"job_type": "operaciones"},
)
