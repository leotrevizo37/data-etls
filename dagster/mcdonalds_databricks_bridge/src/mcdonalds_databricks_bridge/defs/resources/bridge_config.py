from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional
import json
import os
import re


DEFAULT_BRIDGE_JOB_GROUP = "default"


def _split_csv(value: str) -> List[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return value != 0

    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off", ""}:
        return False

    raise ValueError(f"Cannot coerce value '{value}' to boolean.")


def _sanitize_identifier(value: str) -> str:
    sanitized = re.sub(r"[^0-9A-Za-z_]+", "_", value.strip())
    sanitized = re.sub(r"_+", "_", sanitized).strip("_")
    if not sanitized:
        raise ValueError("Identifier cannot be empty after sanitization.")
    if sanitized[0].isdigit():
        sanitized = "_" + sanitized
    return sanitized


def _default_manifest_path() -> Path:
    return Path(__file__).resolve().parents[2] / "config" / "bridge_tables.json"


@dataclass(frozen=True)
class BridgeTableConfig:
    key: str
    ingestion_name: str
    source_schema: str
    source_table: str
    target_table: str
    primary_keys: List[str]
    updated_at_column: str
    created_at_column: str
    enabled: bool = True
    job_group: str = DEFAULT_BRIDGE_JOB_GROUP
    schedule_cron: Optional[str] = None

    @property
    def incremental_cursor_candidates(self) -> List[str]:
        candidates: List[str] = []
        seen = set()

        for value in [self.updated_at_column, self.created_at_column]:
            normalized = value.strip()
            if not normalized:
                continue

            lookup_key = normalized.lower()
            if lookup_key in seen:
                continue

            seen.add(lookup_key)
            candidates.append(normalized)

        return candidates

    @property
    def source_fqn(self) -> str:
        if self.source_schema:
            return f"{self.source_schema}.{self.source_table}"
        return self.source_table

    @property
    def asset_token(self) -> str:
        return _sanitize_identifier(self.key)

    @property
    def source_asset_name(self) -> str:
        return "src_" + self.asset_token

    @property
    def bridge_asset_name(self) -> str:
        return "dbx_" + self.asset_token + "_bridge"

    @property
    def job_group_token(self) -> str:
        return _sanitize_identifier(self.job_group)

    @property
    def asset_group_name(self) -> str:
        return "bridge_" + self.job_group_token


def _build_config_from_legacy_slot(slot: str) -> BridgeTableConfig:
    slot = slot.upper()
    source_table = os.getenv(f"BRIDGE_TABLE_{slot}_SOURCE_TABLE", f"tabla{slot}")
    target_table = os.getenv(f"BRIDGE_TABLE_{slot}_TARGET_TABLE", source_table)
    primary_keys = _split_csv(os.getenv(f"BRIDGE_TABLE_{slot}_PRIMARY_KEYS", "Id"))
    default_source_schema = os.getenv("BRIDGE_SRCSQL_DB", "")

    if not primary_keys:
        raise ValueError(f"BRIDGE_TABLE_{slot}_PRIMARY_KEYS must not be empty.")

    return BridgeTableConfig(
        key=os.getenv(f"BRIDGE_TABLE_{slot}_KEY", source_table),
        ingestion_name=os.getenv(
            f"BRIDGE_TABLE_{slot}_INGESTION_NAME",
            f"DatabricksBridge_{source_table}",
        ),
        source_schema=os.getenv(f"BRIDGE_TABLE_{slot}_SOURCE_SCHEMA", default_source_schema),
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
        enabled=_coerce_bool(os.getenv(f"BRIDGE_TABLE_{slot}_ENABLED", "true")),
        job_group=os.getenv(f"BRIDGE_TABLE_{slot}_JOB_GROUP", DEFAULT_BRIDGE_JOB_GROUP),
        schedule_cron=os.getenv(f"BRIDGE_TABLE_{slot}_SCHEDULE_CRON") or None,
    )


def _load_manifest_entries(manifest_path: Path) -> List[Dict[str, Any]]:
    with manifest_path.open("r", encoding="utf-8") as handle:
        manifest = json.load(handle)

    if isinstance(manifest, list):
        entries = manifest
    elif isinstance(manifest, dict):
        entries = manifest.get("tables", [])
    else:
        raise ValueError(
            f"Bridge manifest at {manifest_path.as_posix()} must be a JSON object or array."
        )

    if not isinstance(entries, list):
        raise ValueError(
            f"Bridge manifest at {manifest_path.as_posix()} must expose a 'tables' list."
        )

    return entries


def _build_config_from_manifest_entry(entry: Dict[str, Any], index: int) -> BridgeTableConfig:
    source_table = str(entry.get("source_table", "")).strip()
    if not source_table:
        raise ValueError(f"Bridge manifest table entry #{index} is missing 'source_table'.")

    default_source_schema = os.getenv("BRIDGE_SRCSQL_DB", "")
    source_schema = str(entry.get("source_schema", default_source_schema)).strip()
    target_table = str(entry.get("target_table", source_table)).strip() or source_table
    key = str(entry.get("key", target_table)).strip() or target_table

    primary_keys_value = entry.get("primary_keys", ["Id"])
    if isinstance(primary_keys_value, str):
        primary_keys = _split_csv(primary_keys_value)
    else:
        primary_keys = [str(item).strip() for item in primary_keys_value if str(item).strip()]

    if not primary_keys:
        raise ValueError(
            f"Bridge manifest table entry '{key}' must define at least one primary key."
        )

    updated_at_value = entry.get("updated_at_column")
    if updated_at_value is None:
        updated_at_column = "updated_at"
    else:
        updated_at_column = str(updated_at_value).strip()

    created_at_value = entry.get("created_at_column")
    if created_at_value is None:
        created_at_column = "created_at"
    else:
        created_at_column = str(created_at_value).strip()

    return BridgeTableConfig(
        key=key,
        ingestion_name=str(
            entry.get("ingestion_name", f"DatabricksBridge_{source_table}")
        ).strip()
        or f"DatabricksBridge_{source_table}",
        source_schema=source_schema,
        source_table=source_table,
        target_table=target_table,
        primary_keys=primary_keys,
        updated_at_column=updated_at_column,
        created_at_column=created_at_column,
        enabled=_coerce_bool(entry.get("enabled", True)),
        job_group=str(entry.get("job_group", DEFAULT_BRIDGE_JOB_GROUP)).strip()
        or DEFAULT_BRIDGE_JOB_GROUP,
        schedule_cron=str(entry.get("schedule_cron")).strip()
        if entry.get("schedule_cron")
        else None,
    )


def _validate_table_configs(table_configs: List[BridgeTableConfig]) -> None:
    uniqueness_checks = {
        "key": {},
        "ingestion_name": {},
        "source_asset_name": {},
        "bridge_asset_name": {},
    }
    job_group_tokens: Dict[str, str] = {}

    for config in table_configs:
        for field_name, registry in uniqueness_checks.items():
            value = getattr(config, field_name)
            existing = registry.get(value)
            if existing is not None:
                raise ValueError(
                    f"Duplicate bridge table configuration for {field_name}='{value}' "
                    f"between '{existing}' and '{config.key}'."
                )
            registry[value] = config.key

        existing_group = job_group_tokens.get(config.job_group_token)
        if existing_group is not None and existing_group != config.job_group:
            raise ValueError(
                "Bridge job groups collide after sanitization: "
                f"'{existing_group}' and '{config.job_group}'."
            )
        job_group_tokens[config.job_group_token] = config.job_group


def _load_bridge_table_configs() -> List[BridgeTableConfig]:
    configured_manifest_path = os.getenv("BRIDGE_TABLES_CONFIG_PATH")

    if configured_manifest_path:
        manifest_path = Path(configured_manifest_path).expanduser()
        if not manifest_path.is_file():
            raise FileNotFoundError(
                "Bridge manifest file was configured but not found at "
                + manifest_path.as_posix()
            )
        table_configs = [
            _build_config_from_manifest_entry(entry, index=index)
            for index, entry in enumerate(_load_manifest_entries(manifest_path), start=1)
        ]
    else:
        default_manifest_path = _default_manifest_path()
        if default_manifest_path.is_file():
            table_configs = [
                _build_config_from_manifest_entry(entry, index=index)
                for index, entry in enumerate(
                    _load_manifest_entries(default_manifest_path),
                    start=1,
                )
            ]
        else:
            table_configs = [
                _build_config_from_legacy_slot("A"),
                _build_config_from_legacy_slot("B"),
                _build_config_from_legacy_slot("C"),
            ]

    enabled_configs = [config for config in table_configs if config.enabled]
    _validate_table_configs(enabled_configs)
    return enabled_configs


@lru_cache(maxsize=1)
def load_table_configs() -> List[BridgeTableConfig]:
    return _load_bridge_table_configs()


def load_table_config(identifier: str) -> BridgeTableConfig:
    normalized = identifier.strip().lower()
    for config in load_table_configs():
        if normalized in {
            config.key.lower(),
            config.source_table.lower(),
            config.target_table.lower(),
            config.asset_token.lower(),
        }:
            return config

    raise KeyError(f"Bridge table configuration '{identifier}' was not found.")
