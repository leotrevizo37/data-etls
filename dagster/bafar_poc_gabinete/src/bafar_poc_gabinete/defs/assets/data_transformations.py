from typing import Optional, Dict, Any

from dagster import asset, AssetKey


@asset(
    name="data_transformations",
    deps=[
        AssetKey(["bafar_poc_gabinete", "dwh_dimsidonproddimensions"]),
        AssetKey(["bafar_poc_gabinete", "dwh_factlecturas"]),
    ],
    group_name="bafar_poc_gabinete",
    key_prefix=["bafar_poc_gabinete"],
)
def data_transformations() -> Optional[Dict[str, Any]]:
    return {"transformations": "magic"}
