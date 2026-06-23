import subprocess
import unittest
import warnings
from datetime import datetime
from importlib import util
from pathlib import Path

try:
    from pyspark.sql import SparkSession
except Exception:
    SparkSession = None


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _java_major_version():
    try:
        result = subprocess.run(
            ["java", "-version"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except Exception:
        return None

    version_text = (result.stderr or result.stdout or "").splitlines()
    if not version_text:
        return None

    first_line = version_text[0]
    marker = 'version "'
    if marker not in first_line:
        return None
    version = first_line.split(marker, 1)[1].split('"', 1)[0]
    try:
        if version.startswith("1."):
            return int(version.split(".")[1])
        return int(version.split(".", 1)[0])
    except ValueError:
        return None


SPARK_TESTS_AVAILABLE = SparkSession is not None and (_java_major_version() or 0) >= 17

if SPARK_TESTS_AVAILABLE:
    path = PROJECT_ROOT / "src" / "carlsjr" / "defs" / "transformations" / "mantiz_tickets.py"
    spec = util.spec_from_file_location("carlsjr_mantiz_tickets", path)
    mantiz_module = util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mantiz_module)
    build_mantiz_ticket_lifecycle_df = mantiz_module.build_mantiz_ticket_lifecycle_df
    build_mantiz_ticket_text_insights_df = mantiz_module.build_mantiz_ticket_text_insights_df
    build_mantiz_text_insight_review_queue_df = (
        mantiz_module.build_mantiz_text_insight_review_queue_df
    )
    LIFECYCLE_COLUMNS = mantiz_module.LIFECYCLE_COLUMNS
    INSIGHT_COLUMNS = mantiz_module.INSIGHT_COLUMNS
    REVIEW_QUEUE_COLUMNS = mantiz_module.REVIEW_QUEUE_COLUMNS
else:
    build_mantiz_ticket_lifecycle_df = None
    build_mantiz_ticket_text_insights_df = None
    build_mantiz_text_insight_review_queue_df = None
    LIFECYCLE_COLUMNS = []
    INSIGHT_COLUMNS = []
    REVIEW_QUEUE_COLUMNS = []


warnings.simplefilter("ignore", ResourceWarning)


TICKET_SCHEMA = """
id string,
branchId string,
equipmentId string,
failureId string,
supplierId string,
folio string,
status string,
priority string,
type string,
source string,
createdAt timestamp,
scheduledAt timestamp,
resolvedAt timestamp,
approvedAt timestamp,
cancelledAt timestamp,
updatedAt timestamp,
photo string,
resolutionPhoto string,
title string,
description string,
scheduleNotes string,
resolutionNotes string,
approvalNotes string,
cancellationNotes string
"""

COMMENT_SCHEMA = """
id string,
ticketId string,
content string,
createdAt timestamp,
updatedAt timestamp
"""


@unittest.skipIf(
    not SPARK_TESTS_AVAILABLE,
    "pyspark or Java 17+ is not available in this environment",
)
class MantizTicketsTransformationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("carlsjr-mantiz-tickets-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.ui.showConsoleProgress", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def _context_frames(self):
        branches = self.spark.createDataFrame(
            [("b1", "c1", "Sucursal 1")],
            "id string, clientId string, name string",
        )
        clients = self.spark.createDataFrame(
            [("c1", "org-carlsjr")],
            "id string, organizationId string",
        )
        equipment_instances = self.spark.createDataFrame(
            [("e1", "ec1", "Parrilla cocina", "A-1", "SN-1")],
            "id string, catalogId string, location string, customAssetId string, serialNumber string",
        )
        equipment_catalog = self.spark.createDataFrame(
            [("ec1", "Parrilla")],
            "id string, name string",
        )
        failure_types = self.spark.createDataFrame(
            [("f1", "Falla general")],
            "id string, name string",
        )
        suppliers = self.spark.createDataFrame(
            [("s1", "Proveedor 1")],
            "id string, name string",
        )
        return (
            branches,
            clients,
            equipment_instances,
            equipment_catalog,
            failure_types,
            suppliers,
        )

    def _empty_comments(self):
        return self.spark.createDataFrame([], COMMENT_SCHEMA)

    def test_lifecycle_calculates_phase_durations_and_preserves_status(self) -> None:
        tickets = self.spark.createDataFrame(
            [
                {
                    "id": "t-open",
                    "branchId": "b1",
                    "equipmentId": "e1",
                    "failureId": "f1",
                    "supplierId": "s1",
                    "folio": "F-1",
                    "status": "ABIERTO",
                    "priority": "Alta",
                    "type": "CORRECTIVO",
                    "source": "APP",
                    "createdAt": datetime(2026, 1, 1, 0, 0),
                    "scheduledAt": None,
                    "resolvedAt": None,
                    "approvedAt": None,
                    "cancelledAt": None,
                    "updatedAt": datetime(2026, 1, 1, 1, 0),
                    "photo": "",
                    "resolutionPhoto": "",
                    "title": "",
                    "description": "",
                    "scheduleNotes": "",
                    "resolutionNotes": "",
                    "approvalNotes": "",
                    "cancellationNotes": "",
                },
                {
                    "id": "t-approved",
                    "branchId": "b1",
                    "equipmentId": "e1",
                    "failureId": "f1",
                    "supplierId": "s1",
                    "folio": "F-2",
                    "status": "APROBADO",
                    "priority": "Media",
                    "type": "PREVENTIVO",
                    "source": "WEB",
                    "createdAt": datetime(2026, 1, 1, 0, 0),
                    "scheduledAt": datetime(2026, 1, 1, 2, 0),
                    "resolvedAt": datetime(2026, 1, 1, 5, 0),
                    "approvedAt": datetime(2026, 1, 1, 8, 0),
                    "cancelledAt": None,
                    "updatedAt": datetime(2026, 1, 1, 8, 0),
                    "photo": "photo.jpg",
                    "resolutionPhoto": "resolution.jpg",
                    "title": "",
                    "description": "",
                    "scheduleNotes": "",
                    "resolutionNotes": "",
                    "approvalNotes": "",
                    "cancellationNotes": "",
                },
                {
                    "id": "t-cancelled",
                    "branchId": "b1",
                    "equipmentId": None,
                    "failureId": None,
                    "supplierId": None,
                    "folio": "F-3",
                    "status": "CANCELADO",
                    "priority": "Baja",
                    "type": "CORRECTIVO",
                    "source": "APP",
                    "createdAt": datetime(2026, 1, 1, 0, 0),
                    "scheduledAt": None,
                    "resolvedAt": None,
                    "approvedAt": None,
                    "cancelledAt": datetime(2026, 1, 1, 4, 0),
                    "updatedAt": datetime(2026, 1, 1, 4, 0),
                    "photo": "",
                    "resolutionPhoto": "",
                    "title": "",
                    "description": "",
                    "scheduleNotes": "",
                    "resolutionNotes": "",
                    "approvalNotes": "",
                    "cancellationNotes": "",
                },
                {
                    "id": "t-orphan",
                    "branchId": "missing",
                    "equipmentId": "e1",
                    "failureId": "f1",
                    "supplierId": "s1",
                    "folio": "F-4",
                    "status": "ABIERTO",
                    "priority": "Alta",
                    "type": "CORRECTIVO",
                    "source": "APP",
                    "createdAt": datetime(2026, 1, 1, 0, 0),
                    "scheduledAt": None,
                    "resolvedAt": None,
                    "approvedAt": None,
                    "cancelledAt": None,
                    "updatedAt": datetime(2026, 1, 1, 1, 0),
                    "photo": "",
                    "resolutionPhoto": "",
                    "title": "",
                    "description": "",
                    "scheduleNotes": "",
                    "resolutionNotes": "",
                    "approvalNotes": "",
                    "cancellationNotes": "",
                },
            ],
            TICKET_SCHEMA,
        )
        frames = self._context_frames()

        result = build_mantiz_ticket_lifecycle_df(
            tickets_df=tickets,
            branches_df=frames[0],
            clients_df=frames[1],
            equipment_instances_df=frames[2],
            equipment_catalog_df=frames[3],
            failure_types_df=frames[4],
            suppliers_df=frames[5],
            organization_id="org-carlsjr",
            as_of_timestamp="2026-01-02 00:00:00",
        )

        self.assertEqual(result.columns, LIFECYCLE_COLUMNS)
        self.assertEqual(result.count(), 3)
        self.assertEqual(
            {row.TicketId for row in result.select("TicketId").collect()},
            {"t-open", "t-approved", "t-cancelled"},
        )

        approved = result.filter("TicketId = 't-approved'").collect()[0]
        self.assertEqual(approved.TicketStatus, "APROBADO")
        self.assertEqual(float(approved.OpenToScheduledHours), 2.0)
        self.assertEqual(float(approved.ScheduledToResolvedHours), 3.0)
        self.assertEqual(float(approved.ResolvedToApprovedHours), 3.0)
        self.assertEqual(float(approved.OpenToResolvedHours), 5.0)
        self.assertEqual(float(approved.OpenToApprovedHours), 8.0)
        self.assertTrue(approved.IsApproved)
        self.assertTrue(approved.IsTerminal)
        self.assertTrue(approved.HasPhoto)
        self.assertTrue(approved.HasResolutionPhoto)
        self.assertIsNone(approved.CurrentAgeHours)

        open_ticket = result.filter("TicketId = 't-open'").collect()[0]
        self.assertTrue(open_ticket.IsOpen)
        self.assertEqual(float(open_ticket.CurrentAgeHours), 24.0)

        cancelled = result.filter("TicketId = 't-cancelled'").collect()[0]
        self.assertTrue(cancelled.IsCancelled)
        self.assertTrue(cancelled.IsTerminal)
        self.assertFalse(cancelled.HasEquipment)
        self.assertFalse(cancelled.HasFailureType)
        self.assertEqual(float(cancelled.OpenToCancelledHours), 4.0)

    def test_text_insights_extract_failures_solutions_and_solution_parts(self) -> None:
        tickets = self.spark.createDataFrame(
            [
                {
                    "id": "t-text",
                    "branchId": "b1",
                    "equipmentId": "e1",
                    "failureId": "f1",
                    "supplierId": "s1",
                    "folio": "F-10",
                    "status": "RESUELTO",
                    "priority": "Alta",
                    "type": "CORRECTIVO",
                    "source": "APP",
                    "createdAt": datetime(2026, 1, 1, 0, 0),
                    "scheduledAt": None,
                    "resolvedAt": datetime(2026, 1, 1, 2, 0),
                    "approvedAt": None,
                    "cancelledAt": None,
                    "updatedAt": datetime(2026, 1, 1, 2, 0),
                    "photo": "",
                    "resolutionPhoto": "",
                    "title": "Parrilla no calienta",
                    "description": "refrigerador no enfria",
                    "scheduleNotes": "",
                    "resolutionNotes": "se cambia reten",
                    "approvalNotes": "se apreto llave",
                    "cancellationNotes": "se acomoda termostato",
                }
            ],
            TICKET_SCHEMA,
        )
        frames = self._context_frames()

        result = build_mantiz_ticket_text_insights_df(
            tickets_df=tickets,
            ticket_comments_df=self._empty_comments(),
            branches_df=frames[0],
            clients_df=frames[1],
            equipment_instances_df=frames[2],
            equipment_catalog_df=frames[3],
            failure_types_df=frames[4],
            suppliers_df=frames[5],
            organization_id="org-carlsjr",
        )

        self.assertEqual(result.columns, INSIGHT_COLUMNS)
        self.assertTrue(
            {"NO_CALIENTA", "NO_ENFRIA"}.issubset(
                {
                    row.CanonicalFailureCode
                    for row in result.filter("InsightType = 'FALLA'").collect()
                }
            ),
        )
        self.assertEqual(
            {
                row.CanonicalSolutionCode
                for row in result.filter("InsightType = 'SOLUCION'").collect()
            },
            {"CAMBIO_REFACCION", "AJUSTE"},
        )
        self.assertEqual(
            {
                row.CanonicalSolutionPartName
                for row in result.filter("InsightType = 'REFACCION_SOLUCION'").collect()
            },
            {"RETEN", "LLAVE", "TERMOSTATO"},
        )
        self.assertTrue(
            all(
                row.IsNewConceptCandidate
                for row in result.select("IsNewConceptCandidate").collect()
            )
        )

        review_queue = build_mantiz_text_insight_review_queue_df(result)
        self.assertEqual(review_queue.columns, REVIEW_QUEUE_COLUMNS)
        self.assertTrue(
            {"FALLA", "SOLUCION", "REFACCION_SOLUCION"}.issubset(
                {row.InsightType for row in review_queue.select("InsightType").collect()}
            )
        )

    def test_empty_text_does_not_generate_insights(self) -> None:
        tickets = self.spark.createDataFrame(
            [
                {
                    "id": "t-empty",
                    "branchId": "b1",
                    "equipmentId": "e1",
                    "failureId": "f1",
                    "supplierId": "s1",
                    "folio": "F-11",
                    "status": "ABIERTO",
                    "priority": "Alta",
                    "type": "CORRECTIVO",
                    "source": "APP",
                    "createdAt": datetime(2026, 1, 1, 0, 0),
                    "scheduledAt": None,
                    "resolvedAt": None,
                    "approvedAt": None,
                    "cancelledAt": None,
                    "updatedAt": datetime(2026, 1, 1, 1, 0),
                    "photo": "",
                    "resolutionPhoto": "",
                    "title": " ",
                    "description": "",
                    "scheduleNotes": "",
                    "resolutionNotes": "",
                    "approvalNotes": "",
                    "cancellationNotes": "",
                }
            ],
            TICKET_SCHEMA,
        )
        frames = self._context_frames()

        result = build_mantiz_ticket_text_insights_df(
            tickets_df=tickets,
            ticket_comments_df=self._empty_comments(),
            branches_df=frames[0],
            clients_df=frames[1],
            equipment_instances_df=frames[2],
            equipment_catalog_df=frames[3],
            failure_types_df=frames[4],
            suppliers_df=frames[5],
            organization_id="org-carlsjr",
        )

        self.assertEqual(result.count(), 0)


if __name__ == "__main__":
    unittest.main()
