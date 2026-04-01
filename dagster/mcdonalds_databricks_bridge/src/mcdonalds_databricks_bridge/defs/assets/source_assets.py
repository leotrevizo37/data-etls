from __future__ import annotations

from typing import Iterable, List

from dagster import AssetKey, SourceAsset

from ..resources import BridgeTableConfig


def build_source_assets(table_configs: Iterable[BridgeTableConfig]) -> List[SourceAsset]:
    return [
        SourceAsset(
            key=AssetKey(table_config.source_asset_name),
            description="Logical source placeholder for Databricks bridge ingestion.",
            group_name="smartaudits_bridge",
        )
        for table_config in table_configs
    ]
