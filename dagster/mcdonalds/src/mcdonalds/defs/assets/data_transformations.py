from typing import Optional, Dict, Any

from dagster import asset, AssetKey

@asset(
    name="data_transformations",
    deps=[
        AssetKey("dwh_dimsidonproddimensions"),
        AssetKey("dwh_factlecturas"),
    ],
    group_name="mcdonalds",
    key_prefix=["mcdonalds"],
)
def data_transformations() -> Optional[Dict[str, Any]]:
    return {"transformations": "magic"}
