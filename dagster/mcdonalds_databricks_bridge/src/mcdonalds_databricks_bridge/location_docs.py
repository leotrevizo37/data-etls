from __future__ import annotations

from typing import Iterable

from dagster.components.core.load_defs import PLUGIN_COMPONENT_TYPES_JSON_METADATA_KEY
from dagster.components.core.snapshot import get_package_entry_snap
from dagster_shared.serdes.objects.package_entry import EnvRegistryKey, json_for_all_components

from .components import (
    BridgeAssetsComponent,
    BridgeCodeLocationComponent,
    BridgeOperationsComponent,
)


def build_location_docs_metadata() -> dict:
    component_types: Iterable[type] = (
        BridgeCodeLocationComponent,
        BridgeAssetsComponent,
        BridgeOperationsComponent,
    )

    snaps = [
        get_package_entry_snap(
            EnvRegistryKey(namespace="mcdonalds_databricks_bridge", name=component_type.__name__),
            component_type,
        )
        for component_type in component_types
    ]

    return {
        PLUGIN_COMPONENT_TYPES_JSON_METADATA_KEY: json_for_all_components(snaps),
    }
