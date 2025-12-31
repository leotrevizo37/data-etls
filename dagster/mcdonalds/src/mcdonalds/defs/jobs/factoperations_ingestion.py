from dagster import define_asset_job, AssetSelection

fact_operaciones_ingestion_job = define_asset_job(
    name="fact_operaciones_ingestion_job",
    selection=AssetSelection.keys("dwh_factReadingsMeasurement"),
)
