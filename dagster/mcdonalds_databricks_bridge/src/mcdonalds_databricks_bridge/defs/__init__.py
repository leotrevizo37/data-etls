"""Dagster definitions for the Databricks bridge.

The bridge owns:
- logical source placeholders for each operational source table,
- append-only bronze ingestion assets,
- grouped jobs and schedules for dimensions and facts,
- cleanup routines for historical Dagster run metadata.
"""
