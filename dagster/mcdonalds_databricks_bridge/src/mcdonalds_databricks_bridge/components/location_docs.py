"""Documentation-only component types for the bridge code location."""

from dagster import Component, Definitions


class BridgeCodeLocationComponent(Component):
    """Bridge code location that lands operational source tables in Databricks bronze.

    This code location runs inside the corporate network, reads source tables from the
    configured operational database, writes parquet batches to a Unity Catalog volume,
    and ingests them into append-only bronze tables through Databricks SQL.

    The bridge is intentionally narrow in scope:
    - move source records reliably from the local network to Databricks,
    - preserve row history in bronze,
    - keep state and retries operationally simple,
    - leave deduplication and business transformations to downstream Databricks layers.
    """

    def build_defs(self, context) -> Definitions:
        del context
        return Definitions()


class BridgeAssetsComponent(Component):
    """Asset pattern used by the bridge for every configured source table.

    Each table in the manifest generates:
    - one logical `src_*` source asset for lineage and organization,
    - one `dbx_*_bridge` asset that performs extraction and Databricks ingestion.

    During a materialization the bridge:
    1. reads the table schema from the source database,
    2. extracts a batch to parquet, incrementally when a cursor column is available,
    3. uploads the file to a Databricks volume,
    4. runs `COPY INTO` against the bronze target table,
    5. records the load outcome and watermark locally.
    """

    def build_defs(self, context) -> Definitions:
        del context
        return Definitions()


class BridgeOperationsComponent(Component):
    """Operational controls for bridge execution, state, and maintenance.

    The bridge stores ingestion state outside Databricks so runs remain idempotent and easy
    to resume from the local environment. It also documents cleanup and orchestration
    responsibilities inside the same code location without changing the bridge load logic.
    """

    def build_defs(self, context) -> Definitions:
        del context
        return Definitions()
