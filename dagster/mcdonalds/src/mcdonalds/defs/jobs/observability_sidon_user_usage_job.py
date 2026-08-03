from dagster import AssetKey, AssetSelection, define_asset_job


observability_sidon_user_usage_job = define_asset_job(
    name="121_observability_sidon_user_usage",
    selection=AssetSelection.keys(
        AssetKey(["mcdonalds", "observability_factsidonuserusage"]),
    ),
    tags={"job_type": "observability", "user_code": "mcdonalds"},
)

