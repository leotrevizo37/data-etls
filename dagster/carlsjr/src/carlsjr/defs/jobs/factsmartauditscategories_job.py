from dagster import AssetKey, AssetSelection, define_asset_job


fact_smartaudits_categories_job = define_asset_job(
    name="fact_smartaudits_categories_job",
    selection=AssetSelection.keys(
        AssetKey(["carlsjr", "dwh_factsmartauditscategories"]),
    ),
    tags={"job_type": "smartaudits", "user_code": "carlsjr"},
)
