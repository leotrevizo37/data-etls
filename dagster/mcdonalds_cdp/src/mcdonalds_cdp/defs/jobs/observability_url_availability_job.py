from dagster import AssetKey, AssetSelection, define_asset_job


observability_url_availability_job = define_asset_job(
    name="122_observability_url_availability",
    selection=AssetSelection.keys(
        AssetKey(["mcdonalds_cdp", "observability_facturlavailabilitydaily"]),
    ),
    tags={"job_type": "url_availability", "user_code_run_lane": "mcdonalds_cdp_observability", "user_code": "mcdonalds_cdp"},
)

