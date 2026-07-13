from typing import Optional, Dict, Any

from dagster import asset, AssetKey


@asset(
    name="data_transformations",
    deps=[
        AssetKey(["emerson", "dwh_dimsidonproddimensions"]),
        AssetKey(["emerson", "dwh_factlecturas"]),
    ],
    group_name="emerson",
    key_prefix=["emerson"],
)
def data_transformations() -> Optional[Dict[str, Any]]:
    return {"transformations": "magic"}
