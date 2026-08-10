from dagster import AssetKey, AssetSelection, define_asset_job


fact_redings_audits_job = define_asset_job(
    name="022_sense_redings_audits",
    selection=AssetSelection.keys(
        AssetKey(["bafar_poc_gabinete", "observability_factredingsaudits"]),
    ),
    tags={"job_type": "observability", "run_lane": "observability", "user_code_run_lane": "bafar_poc_gabinete_observability", "user_code": "bafar_poc_gabinete"},
)
