from dagster import AssetKey, AssetSelection, define_asset_job


fact_sensor_events_job = define_asset_job(
    name="fact_sensor_events_job",
    selection=AssetSelection.keys(
        AssetKey(["carlsjr", "dwh_factsensorevents"]),
    ),
    tags={"job_type": "lecturas", "user_code": "carlsjr"},
)
