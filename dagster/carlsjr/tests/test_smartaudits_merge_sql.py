import importlib.util
import sys
import types
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _install_stubs() -> None:
    if "pyodbc" not in sys.modules:
        pyodbc = types.ModuleType("pyodbc")
        pyodbc.Connection = object
        pyodbc.Error = Exception
        sys.modules["pyodbc"] = pyodbc

    dagster = sys.modules.get("dagster")
    if dagster is None:
        dagster = types.ModuleType("dagster")
        sys.modules["dagster"] = dagster
    if not hasattr(dagster, "resource"):
        def resource(fn=None, **_kwargs):
            return fn if fn else lambda wrapped: wrapped
        dagster.resource = resource


def _load_sql_server_module():
    _install_stubs()
    path = PROJECT_ROOT / "src" / "carlsjr" / "defs" / "resources" / "sql_server.py"
    spec = importlib.util.spec_from_file_location("carlsjr_sql_server_resource", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class SmartAuditsMergeSqlTests(unittest.TestCase):
    def test_merge_sql_is_idempotent_by_plan_result_id(self) -> None:
        module = _load_sql_server_module()
        sql = module.build_fact_smartaudits_categories_merge_sql()

        self.assertIn("MERGE dwh.factSmartauditsCategories", sql)
        self.assertIn("target.PlanResultId = source.PlanResultId", sql)
        self.assertIn("target.Priority = source.Priority", sql)
        self.assertIn("WHEN MATCHED THEN", sql)
        self.assertIn("WHEN NOT MATCHED BY TARGET THEN", sql)
        self.assertIn("WHERE RunId = ?", sql)

    def test_review_queue_merge_sql_is_idempotent_by_hash_and_ai_result(self) -> None:
        module = _load_sql_server_module()
        sql = module.build_smartaudits_comment_review_queue_merge_sql()

        self.assertIn("MERGE ctl.SmartauditsAiCommentReviewQueue", sql)
        self.assertIn("target.NormalizedCommentHash = source.NormalizedCommentHash", sql)
        self.assertIn("target.AiResult = source.AiResult", sql)
        self.assertIn("WHEN MATCHED THEN", sql)
        self.assertIn("WHEN NOT MATCHED BY TARGET THEN", sql)
        self.assertIn("WHERE RunId = ?", sql)

    def test_review_queue_promotion_sql_merges_human_lookup(self) -> None:
        module = _load_sql_server_module()
        sql = module.build_smartaudits_comment_lookup_promotion_sql()

        self.assertIn("MERGE dwh.dimSmartauditsAiCommentLookup", sql)
        self.assertIn("target.NormalizedCommentHash = source.NormalizedCommentHash", sql)
        self.assertIn("target.AiResult = source.AiResult", sql)
        self.assertIn("target.ClassificationMethod = N'HUMAN'", sql)
        self.assertIn("ReviewStatus = N'PROMOTED'", sql)


if __name__ == "__main__":
    unittest.main()
