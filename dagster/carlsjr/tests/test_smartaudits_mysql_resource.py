import importlib.util
import sys
import types
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


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
    path = PROJECT_ROOT / "src" / "carlsjr" / "defs" / "resources" / "smartaudits_mysql.py"
    spec = importlib.util.spec_from_file_location("carlsjr_smartaudits_mysql", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class SmartAuditsMySqlResourceTests(unittest.TestCase):
    def test_jdbc_url_allows_public_key_retrieval(self) -> None:
        module = _load_module()
        resource = module.SmartAuditsMySqlResource(
            host="mysql-host",
            port=3306,
            database="smartaudits",
            user="user",
            password="password",
            driver="com.mysql.cj.jdbc.Driver",
            fetch_size=10000,
            use_ssl="false",
            allow_public_key_retrieval="true",
        )

        options = resource.get_basic_options()

        self.assertIn("allowPublicKeyRetrieval=true", options["url"])
        self.assertIn("useSSL=false", options["url"])
        self.assertEqual(options["driver"], "com.mysql.cj.jdbc.Driver")


if __name__ == "__main__":
    unittest.main()
