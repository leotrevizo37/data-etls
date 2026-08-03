from dagster import AssetKey, AssetSelection, define_asset_job


fact_lecturas_ingestion_job = define_asset_job(
    name="010_sense_lecturas_ingestion",
    selection=AssetSelection.keys(AssetKey(["emerson", "dwh_factlecturas"])),
    tags={"job_type": "lecturas", "run_lane": "operational", "user_code": "emerson"},
)
