from dagster import AssetKey, AssetSelection, define_asset_job


fact_lecturas_operations_job = define_asset_job(
    name="fact_lecturas_operations_job",
    selection=AssetSelection.keys(
        AssetKey(["mcdonalds", "dwh_factlecturas"]),
        AssetKey(["mcdonalds", "data_transformations"]),
        AssetKey(["mcdonalds", "dwh_factReadingsMeasurement"]),
        AssetKey(["mcdonalds", "ctl_IngestionControl"]),
    ),
    tags={"job_type": "lecturas_operaciones", "user_code": "mcdonalds"},
)
