from dagster import AssetKey, AssetSelection, define_asset_job

fact_operations_tables_job = define_asset_job(
    name="020_sense_operations",
    selection=AssetSelection.keys(
        AssetKey(["carlsjr", "data_transformations"]),
        AssetKey(["carlsjr", "dwh_factReadingsMeasurement"]),
        AssetKey(["carlsjr", "ctl_IngestionControl"])
    ),
    tags={"job_type": "operaciones", "user_code": "carlsjr"},
)
