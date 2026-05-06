from typing import Optional, Dict, Any

from dagster import asset, AssetKey

@asset(
    name="data_transformations",
    deps=[
        AssetKey(["mcdonalds_cdp", "dwh_dimsidonproddimensions"]),
        AssetKey(["mcdonalds_cdp", "dwh_factlecturas"]),
    ],
    group_name="mcdonalds_cdp",
    key_prefix=["mcdonalds_cdp"],
)
def data_transformations() -> Optional[Dict[str, Any]]:
    return {"transformations": "magic"}
