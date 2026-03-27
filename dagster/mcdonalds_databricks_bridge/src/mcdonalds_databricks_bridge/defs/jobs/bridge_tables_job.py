from dagster import AssetSelection, define_asset_job

bridge_tables_job = define_asset_job(
    name="bridge_tables_job",
    selection=AssetSelection.keys(
        "dbx_tablaA_bridge",
        "dbx_tablaB_bridge",
        "dbx_tablaC_bridge",
    ),
    tags={"job_type": "databricks_bridge"},
)
