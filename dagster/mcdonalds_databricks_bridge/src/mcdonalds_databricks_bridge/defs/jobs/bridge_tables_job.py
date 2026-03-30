from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Sequence

from dagster import AssetSelection, define_asset_job

from ..resources import BridgeTableConfig


def build_bridge_jobs(table_configs: Sequence[BridgeTableConfig]) -> List:
    if not table_configs:
        return []

    jobs = [
        define_asset_job(
            name="bridge_tables_job",
            selection=AssetSelection.keys(
                *[table_config.bridge_asset_name for table_config in table_configs]
            ),
            tags={
                "job_type": "databricks_bridge",
                "bridge_scope": "all",
            },
        )
    ]

    grouped_configs: Dict[str, List[BridgeTableConfig]] = defaultdict(list)
    for table_config in table_configs:
        grouped_configs[table_config.job_group].append(table_config)

    if len(grouped_configs) == 1:
        return jobs

    for group_name, configs in sorted(grouped_configs.items(), key=lambda item: item[0]):
        jobs.append(
            define_asset_job(
                name="bridge_" + configs[0].job_group_token + "_job",
                selection=AssetSelection.keys(
                    *[table_config.bridge_asset_name for table_config in configs]
                ),
                tags={
                    "job_type": "databricks_bridge",
                    "bridge_scope": "group",
                    "bridge_group": group_name,
                },
            )
        )

    return jobs
