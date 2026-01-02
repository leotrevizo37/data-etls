from dagster import define_asset_job, AssetSelection

facthourlydevicestatus_ingestion = define_asset_job(
    name="facthourlydevicestatus_ingestion",
    selection=AssetSelection.keys(
        "dwh_factHourlyDeviceStatus"
    ),
)
