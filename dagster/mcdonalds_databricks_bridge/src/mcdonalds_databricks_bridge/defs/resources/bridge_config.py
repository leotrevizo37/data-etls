from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Dict, List


def _split_csv(value: str) -> List[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


@dataclass(frozen=True)
class BridgeTableConfig:
    slot: str
    ingestion_name: str
    source_schema: str
    source_table: str
    target_table: str
    primary_keys: List[str]
    updated_at_column: str
    created_at_column: str

    @property
    def source_fqn(self) -> str:
        return f"{self.source_schema}.{self.source_table}"


def load_table_config(slot: str) -> BridgeTableConfig:
    slot = slot.upper()
    source_table = os.getenv(f"BRIDGE_TABLE_{slot}_SOURCE_TABLE", f"tabla{slot}")
    target_table = os.getenv(f"BRIDGE_TABLE_{slot}_TARGET_TABLE", source_table)
    primary_keys = _split_csv(os.getenv(f"BRIDGE_TABLE_{slot}_PRIMARY_KEYS", "Id"))

    if not primary_keys:
        raise ValueError(f"BRIDGE_TABLE_{slot}_PRIMARY_KEYS must not be empty.")

    return BridgeTableConfig(
        slot=slot,
        ingestion_name=os.getenv(
            f"BRIDGE_TABLE_{slot}_INGESTION_NAME",
            f"DatabricksBridge_{source_table}",
        ),
        source_schema=os.getenv(f"BRIDGE_TABLE_{slot}_SOURCE_SCHEMA", "dbo"),
        source_table=source_table,
        target_table=target_table,
        primary_keys=primary_keys,
        updated_at_column=os.getenv(
            f"BRIDGE_TABLE_{slot}_UPDATED_AT_COLUMN",
            "updated_at",
        ),
        created_at_column=os.getenv(
            f"BRIDGE_TABLE_{slot}_CREATED_AT_COLUMN",
            "created_at",
        ),
    )


def load_table_configs() -> Dict[str, BridgeTableConfig]:
    return {
        "A": load_table_config("A"),
        "B": load_table_config("B"),
        "C": load_table_config("C"),
    }
