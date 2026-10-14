from typing import Optional, Dict, Any

from dagster import asset, AssetKey


@asset(
    name="data_transformations",
    deps=[
        AssetKey(["ecosat", "dwh_dimsidonproddimensions"]),
        AssetKey(["ecosat", "dwh_factlecturas"]),
    ],
    group_name="ecosat",
    key_prefix=["ecosat"],
)
def data_transformations() -> Optional[Dict[str, Any]]:
    return {"transformations": "magic"}
