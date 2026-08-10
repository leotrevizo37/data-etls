from dagster import AssetKey, AssetSelection, define_asset_job


observability_dim_aspnet_users_job = define_asset_job(
    name="120_observability_dim_aspnetusers",
    selection=AssetSelection.keys(
        AssetKey(["bafar_poc_gabinete", "observability_dimaspnetusers"]),
    ),
    tags={"job_type": "dimensiones", "user_code_run_lane": "bafar_poc_gabinete_observability", "user_code": "bafar_poc_gabinete"},
)
