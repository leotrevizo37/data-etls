from __future__ import annotations

from typing import Iterable, List

from dagster import AssetKey, SourceAsset

from ..resources import BridgeTableConfig


def build_source_assets(table_configs: Iterable[BridgeTableConfig]) -> List[SourceAsset]:
    return [
        SourceAsset(
            key=AssetKey(table_config.source_asset_name),
            group_name="smartaudits_bridge",
            description=(
                "Logical source placeholder for the bridge extract of `"
                + table_config.source_fqn
                + "`. This asset documents the operational source table that "
                + "feeds the append-only Databricks bronze target `"
                + table_config.target_table
                + "`."
            ),
            metadata={
                "source_table": table_config.source_fqn,
                "target_table": table_config.target_table,
                "job_group": table_config.job_group,
                "primary_key_count": len(table_config.primary_keys),
            },
        )
        for table_config in table_configs
    ]
