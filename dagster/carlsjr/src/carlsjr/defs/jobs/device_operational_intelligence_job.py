from dagster import AssetKey, AssetSelection, define_asset_job, in_process_executor


device_operational_intelligence_job = define_asset_job(
    name="022_sense_device_operational_intelligence",
    selection=AssetSelection.keys(
        AssetKey(["carlsjr", "dwh_factsensorevents"]),
        AssetKey(["carlsjr", "dwh_factDeviceOperationalInsightHourly"]),
        AssetKey(["carlsjr", "dwh_factDeviceOperationalInsightDaily"]),
        AssetKey(["carlsjr", "ctl_SenseDeviceOperationalScoringRuns"]),
        AssetKey(["carlsjr", "ctl_SenseDeviceInsightReviewQueue"]),
    ),
    tags={"job_type": "sense_doi", "user_code": "carlsjr"},
    executor_def=in_process_executor,
)
