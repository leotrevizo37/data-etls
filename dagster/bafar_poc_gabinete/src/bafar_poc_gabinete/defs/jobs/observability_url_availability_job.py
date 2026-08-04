from dagster import AssetKey, AssetSelection, define_asset_job


observability_url_availability_job = define_asset_job(
    name="122_observability_url_availability",
    selection=AssetSelection.keys(
        AssetKey(["bafar_poc_gabinete", "observability_facturlavailabilitydaily"]),
    ),
    tags={"job_type": "observability", "user_code": "bafar_poc_gabinete"},
)

