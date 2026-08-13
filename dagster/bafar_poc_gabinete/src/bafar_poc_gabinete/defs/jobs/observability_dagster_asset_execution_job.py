from dagster import AssetKey, AssetSelection, define_asset_job


observability_dagster_asset_execution_job = define_asset_job(
    name="123_observability_dagster_asset_execution",
    selection=AssetSelection.keys(
        AssetKey(
            ["bafar_poc_gabinete", "observability_factdagsterassetexecutiondaily"]
        ),
    ),
    tags={"job_type": "observability", "run_lane": "observability", "user_code_run_lane": "bafar_poc_gabinete_observability", "user_code": "bafar_poc_gabinete"},
)
