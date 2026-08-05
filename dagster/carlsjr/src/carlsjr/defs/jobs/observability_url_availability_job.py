from dagster import AssetKey, AssetSelection, define_asset_job


observability_url_availability_job = define_asset_job(
    name="122_observability_url_availability",
    selection=AssetSelection.keys(
        AssetKey(["carlsjr", "observability_facturlavailabilitydaily"]),
    ),
    tags={"job_type": "url_availability", "user_code_run_lane": "carlsjr_observability", "user_code": "carlsjr"},
)

