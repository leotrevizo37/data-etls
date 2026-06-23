import importlib.util
import re
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


def _matched_update_section(sql: str) -> str:
    match = re.search(
        r"WHEN MATCHED THEN\s+UPDATE SET(?P<section>.*?)WHEN NOT MATCHED BY TARGET",
        sql,
        flags=re.S,
    )
    assert match is not None
    return match.group("section")


class MantizMergeSqlTests(unittest.TestCase):
    def test_lifecycle_merge_is_idempotent_by_organization_and_ticket(self) -> None:
        module = _load_sql_server_module()
        sql = module.build_fact_mantiz_ticket_lifecycle_merge_sql()

        self.assertIn("MERGE dwh.factMantizTicketLifecycle", sql)
        self.assertIn("target.OrganizationId = source.OrganizationId", sql)
        self.assertIn("target.TicketId = source.TicketId", sql)
        self.assertIn("target.TicketStatus = source.TicketStatus", sql)
        self.assertIn("target.OpenToScheduledHours = source.OpenToScheduledHours", sql)
        self.assertIn("target.CurrentAgeHours = source.CurrentAgeHours", sql)
        self.assertIn("WHERE RunId = ?", sql)

    def test_text_insights_replace_deletes_changed_tickets_before_insert(self) -> None:
        module = _load_sql_server_module()
        sql = module.build_fact_mantiz_ticket_text_insights_replace_sql()

        self.assertIn("DELETE target", sql)
        self.assertIn("FROM dwh.factMantizTicketTextInsights AS target", sql)
        self.assertIn("FROM stg.MantizTicketTextInsightChangedTicket", sql)
        self.assertIn("target.OrganizationId = changed.OrganizationId", sql)
        self.assertIn("target.TicketId = changed.TicketId", sql)
        self.assertIn("INSERT INTO dwh.factMantizTicketTextInsights", sql)
        self.assertIn("FROM stg.factMantizTicketTextInsights", sql)

    def test_failure_concept_merge_preserves_reviewed_status(self) -> None:
        module = _load_sql_server_module()
        sql = module.build_mantiz_failure_concept_merge_sql()
        update_section = _matched_update_section(sql)

        self.assertIn("MERGE dwh.dimMantizFailureConcept", sql)
        self.assertIn("source.FailureConceptHash", sql)
        self.assertNotIn("target.ConceptStatus", update_section)
        self.assertNotIn("N'CANDIDATE'", update_section)

    def test_solution_concept_merge_preserves_reviewed_status(self) -> None:
        module = _load_sql_server_module()
        sql = module.build_mantiz_solution_concept_merge_sql()
        update_section = _matched_update_section(sql)

        self.assertIn("MERGE dwh.dimMantizSolutionConcept", sql)
        self.assertIn("source.SolutionConceptHash", sql)
        self.assertNotIn("target.ConceptStatus", update_section)
        self.assertNotIn("N'CANDIDATE'", update_section)

    def test_solution_part_concept_merge_preserves_reviewed_status(self) -> None:
        module = _load_sql_server_module()
        sql = module.build_mantiz_solution_part_concept_merge_sql()
        update_section = _matched_update_section(sql)

        self.assertIn("MERGE dwh.dimMantizSolutionPartConcept", sql)
        self.assertIn("source.SolutionPartConceptHash", sql)
        self.assertNotIn("target.ConceptStatus", update_section)
        self.assertNotIn("N'CANDIDATE'", update_section)

    def test_review_queue_merge_uses_ctl_and_preserves_review_status(self) -> None:
        module = _load_sql_server_module()
        sql = module.build_mantiz_text_insight_review_queue_merge_sql()
        update_section = _matched_update_section(sql)

        self.assertIn("MERGE ctl.MantizTextInsightReviewQueue", sql)
        self.assertIn("FROM stg.MantizTextInsightReviewQueue", sql)
        self.assertIn("target.InsightType = source.InsightType", sql)
        self.assertIn("target.ConceptHash = source.ConceptHash", sql)
        self.assertNotIn("target.ReviewStatus", update_section)
        self.assertNotIn("target.ReviewedBy", update_section)


if __name__ == "__main__":
    unittest.main()
