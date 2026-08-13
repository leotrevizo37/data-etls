from dagster import AssetKey, AssetSelection, define_asset_job


observability_dagster_asset_execution_job = define_asset_job(
    name="123_observability_dagster_asset_execution",
    selection=AssetSelection.keys(
        AssetKey(
            ["smartfit", "observability_factdagsterassetexecutiondaily"]
        ),
    ),
    tags={"job_type": "observability", "run_lane": "observability", "user_code_run_lane": "smartfit_observability", "user_code": "smartfit"},
)
