from typing import Optional, Dict, Any

from dagster import asset, AssetKey

@asset(
    name="data_transformations",
    deps=[
        AssetKey("dwh_dimsidonproddimensions"),
        AssetKey("dwh_factlecturas"),
    ],
    group_name="sense_operations",
)
def data_transformations() -> Optional[Dict[str, Any]]:
    return {"transformations": "magic"}
