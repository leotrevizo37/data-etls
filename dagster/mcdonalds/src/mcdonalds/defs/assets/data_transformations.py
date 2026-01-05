from typing import Optional, Dict, Any

from dagster import asset, AssetKey, Nothing

@asset(
    name="data_transformations",
    deps=[
        AssetKey("dwh_dimsidonproddimensions"),
        AssetKey("dwh_factlecturas"),
    ],
)
def data_transformations() -> Optional[Dict[str, Any]]:
    return {"transformations": "magic"}
