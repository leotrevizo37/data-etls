from __future__ import annotations

import pytest

pytest.importorskip("databricks.sql")

from mcdonalds_databricks_bridge.definitions import defs as build_defs
from mcdonalds_databricks_bridge.defs.assets import (
    bridge_cleanup_old_runs,
    build_bridge_assets,
    build_source_assets,
)
from mcdonalds_databricks_bridge.defs.jobs import bridge_cleanup_job, build_bridge_jobs
from mcdonalds_databricks_bridge.defs.resources import load_table_configs


def test_bridge_definitions_expose_documentation_metadata():
    definitions = build_defs()

    assert definitions.component_tree is not None
    assert "code_location_summary" in definitions.metadata
    assert definitions.metadata["table_count"].value == len(load_table_configs())


def test_bridge_assets_expose_descriptions():
    table_configs = load_table_configs()

    source_assets = build_source_assets(table_configs)
    bridge_assets = build_bridge_assets(table_configs)

    assert source_assets
    assert all(source_asset.description for source_asset in source_assets)
    assert bridge_assets
    assert all(
        all(description for description in asset_def.descriptions_by_key.values())
        for asset_def in bridge_assets
    )
    assert all(
        description for description in bridge_cleanup_old_runs.descriptions_by_key.values()
    )


def test_bridge_jobs_expose_descriptions():
    bridge_jobs = build_bridge_jobs(load_table_configs())

    assert bridge_jobs
    assert all(job.description for job in bridge_jobs)
    assert bridge_cleanup_job.description
