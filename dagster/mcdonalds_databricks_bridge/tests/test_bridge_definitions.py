import os
import unittest
from tempfile import TemporaryDirectory
from types import SimpleNamespace

from mcdonalds_databricks_bridge.definitions import defs
from dagster.components.core.load_defs import PLUGIN_COMPONENT_TYPES_JSON_METADATA_KEY
from mcdonalds_databricks_bridge.defs.assets.bridge_assets import _sync_table
from mcdonalds_databricks_bridge.defs.jobs import build_bridge_jobs
from mcdonalds_databricks_bridge.defs.resources import BridgeTableConfig
from mcdonalds_databricks_bridge.defs.resources.source_mysql import ExtractResult, SourceColumn
from mcdonalds_databricks_bridge.defs.schedules import build_bridge_schedules


class _FakeBridgeState:
    def __init__(self):
        self.completed = []

    def get_watermark(self, _ingestion_name):
        return None

    def mark_run_started(self, _ingestion_name):
        return None

    def mark_run_completed(self, **kwargs):
        self.completed.append(kwargs)


class _FakeSourceMySql:
    def __init__(self, output_path):
        self.columns = [
            SourceColumn("Id", "int", False, 1),
            SourceColumn("created_at", "datetime", True, 2),
        ]
        self.output_path = output_path

    def get_table_schema(self, _table_config):
        return self.columns

    def extract_incremental_to_parquet(self, **_kwargs):
        return ExtractResult(
            row_count=3,
            max_updated_at=None,
            columns=self.columns,
            output_path=self.output_path,
        )


class _FakeCursor:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeConnection:
    def cursor(self):
        return _FakeCursor()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeDatabricksSql:
    def __init__(self, staging_allowed_local_path):
        self.staging_allowed_local_path = staging_allowed_local_path

    def build_remote_batch_dir(self, table_config, batch_id):
        return f"/Volumes/main/files/bridge/{table_config.target_table}/{batch_id}"

    def build_remote_file_path(self, table_config, batch_id, local_path):
        return self.build_remote_batch_dir(table_config, batch_id) + "/" + os.path.basename(local_path)

    def connect(self):
        return _FakeConnection()

    def ensure_objects(self, **_kwargs):
        return None

    def upload_file(self, **_kwargs):
        return None

    def copy_into_target(self, **_kwargs):
        return None

    def remove_remote_file(self, **_kwargs):
        return None


class BridgeDefinitionsTests(unittest.TestCase):
    def _build_table_config(self, job_group="dims", key="sample_table"):
        return BridgeTableConfig(
            key=key,
            ingestion_name="DatabricksBridge_" + key,
            source_schema="app",
            source_table=key,
            target_table="sa_" + key,
            primary_keys=["Id"],
            updated_at_column="updated_at",
            created_at_column="created_at",
            enabled=True,
            job_group=job_group,
            schedule_cron=None,
        )

    def test_defs_attach_component_tree(self):
        definitions = defs()

        self.assertIsNotNone(definitions.component_tree)
        self.assertEqual(definitions.component_tree.defs_module.__name__, "mcdonalds_databricks_bridge.defs")
        self.assertIn(PLUGIN_COMPONENT_TYPES_JSON_METADATA_KEY, definitions.metadata)
        self.assertTrue(definitions.metadata[PLUGIN_COMPONENT_TYPES_JSON_METADATA_KEY])

    def test_bridge_schedules_use_single_global_cron(self):
        table_configs = [
            self._build_table_config("dims", "sample_dims"),
            self._build_table_config("facts", "sample_facts"),
        ]
        jobs = build_bridge_jobs(table_configs)

        with unittest.mock.patch.dict(
            os.environ,
            {"BRIDGE_SYNC_CRON": "0 */8 * * *"},
            clear=False,
        ):
            schedules = build_bridge_schedules(jobs)

        self.assertEqual(
            {schedule.name for schedule in schedules},
            {"bridge_dims_schedule", "bridge_facts_schedule", "bridge_tables_schedule"},
        )
        cron_by_name = {schedule.name: schedule.cron_schedule for schedule in schedules}
        self.assertEqual(cron_by_name["bridge_dims_schedule"], "0 */8 * * *")
        self.assertEqual(cron_by_name["bridge_facts_schedule"], "0 */8 * * *")
        self.assertEqual(cron_by_name["bridge_tables_schedule"], "0 */8 * * *")

    def test_bridge_asset_emits_numeric_metadata_for_plots(self):
        table_config = self._build_table_config()

        with TemporaryDirectory() as tmp_dir:
            output_path = os.path.join(tmp_dir, "sample.parquet")
            with open(output_path, "wb") as handle:
                handle.write(b"PAR1")

            context = SimpleNamespace(
                resources=SimpleNamespace(
                    source_mysql=_FakeSourceMySql(output_path),
                    bridge_state_sqlserver=_FakeBridgeState(),
                    databricks_sql=_FakeDatabricksSql(tmp_dir),
                ),
                log=SimpleNamespace(info=lambda *args, **kwargs: None),
            )

            result = _sync_table(context, table_config)

        self.assertEqual(result.metadata["row_count"], 3)
        self.assertEqual(result.metadata["rows_loaded"], 3)
        self.assertEqual(result.metadata["column_count"], 2)
        self.assertEqual(result.metadata["remote_files_uploaded"], 1)


if __name__ == "__main__":
    unittest.main()
