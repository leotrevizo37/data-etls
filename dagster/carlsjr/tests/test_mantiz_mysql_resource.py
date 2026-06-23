import importlib.util
import os
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch


PROJECT_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = PROJECT_ROOT.parents[1]


def _install_stubs() -> None:
    dagster = sys.modules.get("dagster")
    if dagster is None:
        dagster = types.ModuleType("dagster")
        sys.modules["dagster"] = dagster
    if not hasattr(dagster, "resource"):
        def resource(fn=None, **_kwargs):
            return fn if fn else lambda wrapped: wrapped
        dagster.resource = resource


def _load_module():
    _install_stubs()
    path = PROJECT_ROOT / "src" / "carlsjr" / "defs" / "resources" / "mantiz_mysql.py"
    spec = importlib.util.spec_from_file_location("carlsjr_mantiz_mysql", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class MantizMySqlResourceTests(unittest.TestCase):
    def test_jdbc_url_uses_mantiz_mysql_options(self) -> None:
        module = _load_module()
        resource = module.MantizMySqlResource(
            host="mysql-host",
            port=3306,
            database="mantiz",
            user="user",
            password="password",
            driver="com.mysql.cj.jdbc.Driver",
            fetch_size=10000,
            use_ssl="false",
            allow_public_key_retrieval="true",
            organization_id="org-1",
        )

        options = resource.get_basic_options()

        self.assertIn("zeroDateTimeBehavior=CONVERT_TO_NULL", options["url"])
        self.assertIn("serverTimezone=UTC", options["url"])
        self.assertIn("allowPublicKeyRetrieval=true", options["url"])
        self.assertIn("useSSL=false", options["url"])
        self.assertEqual(options["driver"], "com.mysql.cj.jdbc.Driver")
        self.assertEqual(resource.organization_id, "org-1")

    def test_resource_reads_mantiz_environment(self) -> None:
        module = _load_module()
        env = {
            "MANTIZ_MYSQL_HOST": "mantiz-host",
            "MANTIZ_MYSQL_PORT": "3307",
            "MANTIZ_MYSQL_DATABASE": "mantiz_db",
            "MANTIZ_MYSQL_USER": "mantiz_user",
            "MANTIZ_MYSQL_PASSWORD": "mantiz_password",
            "MANTIZ_ORGANIZATION_ID": "org-2",
        }

        with patch.dict(os.environ, env, clear=True):
            resource = module.mantiz_mysql_resource(None)

        self.assertEqual(resource.host, "mantiz-host")
        self.assertEqual(resource.port, 3307)
        self.assertEqual(resource.database, "mantiz_db")
        self.assertEqual(resource.user, "mantiz_user")
        self.assertEqual(resource.password, "mantiz_password")
        self.assertEqual(resource.organization_id, "org-2")

    def test_carlsjr_compose_maps_to_container_mantiz_environment(self) -> None:
        compose = (REPO_ROOT / "compose-data_etls" / "docker-compose.yml").read_text()

        self.assertIn(
            "MANTIZ_MYSQL_HOST: ${CARLSJR_MANTIZ_MYSQL_HOST:-${MANTIZ_MYSQL_HOST:-}}",
            compose,
        )
        self.assertIn(
            "MANTIZ_MYSQL_DATABASE: ${CARLSJR_MANTIZ_MYSQL_DATABASE:-${MANTIZ_MYSQL_DATABASE:-}}",
            compose,
        )
        self.assertIn(
            "MANTIZ_ORGANIZATION_ID: ${CARLSJR_MANTIZ_ORGANIZATION_ID:-${MANTIZ_ORGANIZATION_ID:-}}",
            compose,
        )
        self.assertIn(
            "MANTIZ_START_DATE: ${CARLSJR_MANTIZ_START_DATE:-${MANTIZ_START_DATE:-2026-01-01}}",
            compose,
        )

    def test_mantiz_job_is_serialized(self) -> None:
        job_file = (
            PROJECT_ROOT
            / "src"
            / "carlsjr"
            / "defs"
            / "jobs"
            / "factmantiztickets_job.py"
        ).read_text()

        self.assertIn("in_process_executor", job_file)
        self.assertIn("executor_def=in_process_executor", job_file)

    def test_mantiz_sql_init_is_split_by_object(self) -> None:
        init_dir = REPO_ROOT / "sql" / "user_codes" / "carlsjr" / "init"

        self.assertTrue((init_dir / "factMantizTicketLifecycle.sql").exists())
        self.assertTrue((init_dir / "factMantizTicketTextInsights.sql").exists())
        self.assertTrue((init_dir / "MantizTextInsightReviewQueue.sql").exists())
        self.assertTrue((init_dir / "MantizTicketTextInsightDetail.sql").exists())
        self.assertTrue((init_dir / "dropMantizDwhDimensions.sql").exists())
        self.assertFalse(any(path.name[:1].isdigit() for path in init_dir.glob("*.sql")))
        self.assertFalse(any(path.name.startswith("dimMantiz") for path in init_dir.glob("*.sql")))
        self.assertFalse((init_dir / "factMantizTickets.sql").exists())


if __name__ == "__main__":
    unittest.main()
