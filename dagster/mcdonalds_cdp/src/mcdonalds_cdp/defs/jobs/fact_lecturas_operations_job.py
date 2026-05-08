from dagster import AssetKey, AssetSelection, define_asset_job


fact_lecturas_operations_job = define_asset_job(
    name="fact_lecturas_operations_job",
    selection=AssetSelection.keys(
        AssetKey(["mcdonalds_cdp", "dwh_factlecturas"]),
        AssetKey(["mcdonalds_cdp", "data_transformations"]),
        AssetKey(["mcdonalds_cdp", "dwh_factReadingsMeasurement"]),
        AssetKey(["mcdonalds_cdp", "ctl_IngestionControl"]),
    ),
    tags={"job_type": "lecturas_operaciones", "user_code": "mcdonalds_cdp"},
)
