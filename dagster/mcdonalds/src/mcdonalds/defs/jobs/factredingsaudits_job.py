from dagster import AssetKey, AssetSelection, define_asset_job


fact_redings_audits_job = define_asset_job(
    name="022_sense_redings_audits",
    selection=AssetSelection.keys(
        AssetKey(["mcdonalds", "observability_factredingsaudits"]),
    ),
    tags={"job_type": "lecturas", "user_code": "mcdonalds"},
)
