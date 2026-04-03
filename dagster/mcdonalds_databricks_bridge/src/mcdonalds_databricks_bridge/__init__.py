"""Append-only Databricks bridge code location.

This package materializes source-system tables from the local network into
Databricks bronze tables. The bridge focuses on operational reliability:
incremental extracts when possible, append-only ingestion into bronze, and
grouped Dagster jobs/schedules for controlled compute usage.
"""
