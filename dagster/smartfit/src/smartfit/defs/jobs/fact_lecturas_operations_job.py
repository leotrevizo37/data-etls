from dagster import AssetKey, AssetSelection, define_asset_job


fact_lecturas_operations_job = define_asset_job(
    name="fact_lecturas_operations_job",
    selection=AssetSelection.keys(
        AssetKey(["smartfit", "dwh_factlecturas"]),
        AssetKey(["smartfit", "data_transformations"]),
        AssetKey(["smartfit", "dwh_factReadingsMeasurement"]),
        AssetKey(["smartfit", "ctl_IngestionControl"]),
    ),
    tags={"job_type": "lecturas_operaciones", "user_code": "smartfit"},
)
