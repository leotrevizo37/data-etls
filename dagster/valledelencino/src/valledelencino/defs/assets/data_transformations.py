from typing import Optional, Dict, Any

from dagster import asset, AssetKey


@asset(
    name="data_transformations",
    deps=[
        AssetKey(["valledelencino", "dwh_dimsidonproddimensions"]),
        AssetKey(["valledelencino", "dwh_factlecturas"]),
    ],
    group_name="valledelencino",
    key_prefix=["valledelencino"],
)
def data_transformations() -> Optional[Dict[str, Any]]:
    return {"transformations": "magic"}
