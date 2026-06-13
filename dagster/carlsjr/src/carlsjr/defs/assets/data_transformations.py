from typing import Optional, Dict, Any

from dagster import asset, AssetKey

@asset(
    name="data_transformations",
    deps=[
        AssetKey(["carlsjr", "dwh_dimsidonproddimensions"]),
        AssetKey(["carlsjr", "dwh_factlecturas"]),
    ],
    group_name="carlsjr",
    key_prefix=["carlsjr"],
)
def data_transformations() -> Optional[Dict[str, Any]]:
    return {"transformations": "magic"}
