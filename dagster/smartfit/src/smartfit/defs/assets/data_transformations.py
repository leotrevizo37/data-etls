from typing import Optional, Dict, Any

from dagster import asset, AssetKey

@asset(
    name="data_transformations",
    deps=[
        AssetKey(["smartfit", "dwh_dimsidonproddimensions"]),
        AssetKey(["smartfit", "dwh_factlecturas"]),
    ],
    group_name="smartfit",
    key_prefix=["smartfit"],
)
def data_transformations() -> Optional[Dict[str, Any]]:
    return {"transformations": "magic"}
