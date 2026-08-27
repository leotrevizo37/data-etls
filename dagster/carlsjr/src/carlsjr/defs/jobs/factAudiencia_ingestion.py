from dagster import AssetKey, AssetSelection, define_asset_job

fact_audiencia_ingestion_job = define_asset_job(
    name="014_sense_audiencia_ingestion",
    selection=AssetSelection.keys(AssetKey(["carlsjr", "dwh_factaudiencia"])),
    tags={"job_type": "lecturas", "run_lane": "operational", "user_code_run_lane": "carlsjr_operational", "user_code": "carlsjr"},
)
