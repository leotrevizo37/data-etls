import sys
import unittest
import warnings
from datetime import datetime
from importlib import import_module
from pathlib import Path

try:
    from pyspark.sql import SparkSession
except Exception:
    SparkSession = None


PROJECT_SRC = Path(__file__).resolve().parents[1] / "src"
if str(PROJECT_SRC) not in sys.path:
    sys.path.insert(0, str(PROJECT_SRC))

if SparkSession is not None:
    categories_module = import_module("carlsjr.defs.transformations.smartaudits_categories")
    build_smartaudits_categories_df = categories_module.build_smartaudits_categories_df
    build_smartaudits_comment_review_queue_df = (
        categories_module.build_smartaudits_comment_review_queue_df
    )
    empty_classifier_predictions_df = categories_module.empty_classifier_predictions_df
    OUTPUT_COLUMNS = categories_module.OUTPUT_COLUMNS
    REVIEW_QUEUE_COLUMNS = categories_module.REVIEW_QUEUE_COLUMNS
else:
    build_smartaudits_categories_df = None
    build_smartaudits_comment_review_queue_df = None
    empty_classifier_predictions_df = None
    OUTPUT_COLUMNS = []
    REVIEW_QUEUE_COLUMNS = []


warnings.simplefilter("ignore", ResourceWarning)


@unittest.skipIf(SparkSession is None, "pyspark is not installed in this environment")
class SmartAuditsCategoriesTransformationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("carlsjr-smartaudits-categories-tests")
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

    def _base_frames(self):
        plan_results = self.spark.createDataFrame(
            [
                {
                    "id": 1,
                    "workPlanId": 10,
                    "okAI": 0,
                    "reviewDate": datetime(2026, 3, 31, 10, 0),
                    "reviewAIDate": datetime(2026, 3, 31, 10, 5),
                },
                {
                    "id": 2,
                    "workPlanId": 20,
                    "okAI": 1,
                    "reviewDate": datetime(2026, 3, 31, 11, 0),
                    "reviewAIDate": datetime(2026, 3, 31, 11, 5),
                },
                {
                    "id": 3,
                    "workPlanId": 30,
                    "okAI": 0,
                    "reviewDate": datetime(2026, 3, 31, 12, 0),
                    "reviewAIDate": datetime(2026, 3, 31, 12, 5),
                },
            ]
        )
        evidence = self.spark.createDataFrame(
            [
                {
                    "id": 101,
                    "planResultsId": 1,
                    "resultAI": 0,
                    "commentsAI": "No se puede determinar la temperatura visible.",
                    "createdAt": datetime(2026, 3, 31, 10, 30),
                },
                {
                    "id": 102,
                    "planResultsId": 1,
                    "resultAI": 0,
                    "commentsAI": "No se puede detectar el valor del termometro.",
                    "createdAt": datetime(2026, 3, 31, 10, 31),
                },
                {
                    "id": 103,
                    "planResultsId": 1,
                    "resultAI": 0,
                    "commentsAI": "La foto esta borrosa.",
                    "createdAt": datetime(2026, 3, 31, 10, 32),
                },
                {
                    "id": 201,
                    "planResultsId": 2,
                    "resultAI": 1,
                    "commentsAI": "Cumple correctamente.",
                    "createdAt": datetime(2026, 3, 31, 11, 10),
                },
                {
                    "id": 301,
                    "planResultsId": 3,
                    "resultAI": 0,
                    "commentsAI": "Comentario operativo ambiguo",
                    "createdAt": datetime(2026, 3, 31, 12, 10),
                },
            ]
        )
        work_plan = self.spark.createDataFrame(
            [
                {"id": 10, "taskId": 100, "checkpointId": 1000},
                {"id": 20, "taskId": 200, "checkpointId": 2000},
                {"id": 30, "taskId": 300, "checkpointId": 3000},
            ]
        )
        tasks = self.spark.createDataFrame(
            [
                {
                    "id": 100,
                    "description": "Congelador - Temperaturas productos",
                    "priorityTypeId": 1,
                },
                {
                    "id": 200,
                    "description": "Limpieza mostrador",
                    "priorityTypeId": 2,
                },
                {
                    "id": 300,
                    "description": "Revision operativa",
                    "priorityTypeId": 3,
                },
            ]
        )
        checkpoints = self.spark.createDataFrame(
            [
                {"id": 1000, "sublocationId": 500, "description": "Checkpoint A"},
                {"id": 2000, "sublocationId": 500, "description": "Checkpoint B"},
                {"id": 3000, "sublocationId": 500, "description": "Checkpoint C"},
            ]
        )
        sublocations = self.spark.createDataFrame(
            [{"id": 500, "locationId": "loc-1", "description": "Cocina"}]
        )
        locations = self.spark.createDataFrame(
            [{"id": "loc-1", "Name": "Sucursal 1"}]
        )
        priority_types = self.spark.createDataFrame(
            [
                {"id": 1, "name": "Alta"},
                {"id": 2, "name": "Media"},
            ]
        )
        task_categories = self.spark.createDataFrame(
            [
                {"taskId": 100, "categoryId": 5},
                {"taskId": 200, "categoryId": 4},
            ]
        )
        categories = self.spark.createDataFrame(
            [
                {"id": 4, "name": "Limpieza"},
                {"id": 5, "name": "Calidad"},
            ]
        )
        return (
            plan_results,
            evidence,
            work_plan,
            tasks,
            checkpoints,
            sublocations,
            locations,
            priority_types,
            task_categories,
            categories,
        )

    def _result_df(self):
        frames = self._base_frames()
        return build_smartaudits_categories_df(
            plan_results_df=frames[0],
            evidence_photos_df=frames[1],
            work_plan_df=frames[2],
            tasks_df=frames[3],
            checkpoints_df=frames[4],
            sublocations_df=frames[5],
            locations_df=frames[6],
            priority_types_df=frames[7],
            task_categories_df=frames[8],
            categories_df=frames[9],
            classifier_predictions_df=empty_classifier_predictions_df(self.spark),
        )

    def test_output_columns_match_sql_schema_order(self) -> None:
        self.assertEqual(self._result_df().columns, OUTPUT_COLUMNS)

    def test_granularity_is_one_row_per_plan_result(self) -> None:
        result = self._result_df()

        self.assertEqual(result.count(), 3)
        self.assertEqual(
            result.select("PlanResultId").distinct().count(),
            3,
        )

    def test_failed_task_uses_mode_of_failed_evidence_categories(self) -> None:
        row = (
            self._result_df()
            .filter("PlanResultId = 1")
            .select(
                "ResultCategory",
                "EvidencePhotoId",
                "FailedEvidenceCount",
                "EvidenceCount",
            )
            .collect()[0]
        )

        self.assertEqual(row.ResultCategory, "IMAGEN_NO_PROCESABLE")
        self.assertEqual(row.EvidencePhotoId, 101)
        self.assertEqual(row.FailedEvidenceCount, 3)
        self.assertEqual(row.EvidenceCount, 3)

    def test_passed_task_is_compliance_without_representative_evidence(self) -> None:
        row = (
            self._result_df()
            .filter("PlanResultId = 2")
            .select("ResultCategory", "EvidencePhotoId", "ClassifierConfidence")
            .collect()[0]
        )

        self.assertEqual(row.ResultCategory, "CUMPLIMIENTO")
        self.assertIsNone(row.EvidencePhotoId)
        self.assertEqual(row.ClassifierConfidence, 1.0)

    def test_unmatched_failed_comment_falls_back_to_unclassified(self) -> None:
        row = (
            self._result_df()
            .filter("PlanResultId = 3")
            .select("ResultCategory", "ClassificationMethod")
            .collect()[0]
        )

        self.assertEqual(row.ResultCategory, "SIN_CLASIFICAR")
        self.assertEqual(row.ClassificationMethod, "SIN_CLASIFICAR")

    def test_priority_comes_from_task_priority_type(self) -> None:
        row = (
            self._result_df()
            .filter("PlanResultId = 1")
            .select("Priority")
            .collect()[0]
        )

        self.assertEqual(row.Priority, "Alta")

    def test_unmatched_failed_comment_can_use_ml_prediction(self) -> None:
        frames = self._base_frames()
        predictions = self.spark.createDataFrame(
            [
                {
                    "NormalizedComment": "comentario operativo ambiguo",
                    "PredictedCategory": "INCUMPLIMIENTO_GENERAL",
                    "PredictionMethod": "ML",
                    "ClassifierModelVersion": "test-model",
                    "ClassifierConfidence": 0.91,
                }
            ]
        )
        result = build_smartaudits_categories_df(
            plan_results_df=frames[0],
            evidence_photos_df=frames[1],
            work_plan_df=frames[2],
            tasks_df=frames[3],
            checkpoints_df=frames[4],
            sublocations_df=frames[5],
            locations_df=frames[6],
            priority_types_df=frames[7],
            task_categories_df=frames[8],
            categories_df=frames[9],
            classifier_predictions_df=predictions,
        )

        row = (
            result
            .filter("PlanResultId = 3")
            .select("ResultCategory", "ClassificationMethod", "ClassifierModelVersion")
            .collect()[0]
        )

        self.assertEqual(row.ResultCategory, "INCUMPLIMIENTO_GENERAL")
        self.assertEqual(row.ClassificationMethod, "ML")
        self.assertEqual(row.ClassifierModelVersion, "test-model")

    def test_human_lookup_wins_over_deterministic_rule_for_failed_task(self) -> None:
        frames = self._base_frames()
        lookup = self.spark.createDataFrame(
            [
                (
                    "la foto esta borrosa.",
                    0,
                    "INCUMPLIMIENTO_GENERAL",
                    "HUMAN",
                    None,
                    1.0,
                )
            ],
            "NormalizedComment string, AiResult tinyint, ResultCategory string, "
            "ClassificationMethod string, ClassifierModelVersion string, "
            "ClassifierConfidence double",
        )
        result = build_smartaudits_categories_df(
            plan_results_df=frames[0].filter("id = 1"),
            evidence_photos_df=frames[1].filter("id = 103"),
            work_plan_df=frames[2],
            tasks_df=frames[3],
            checkpoints_df=frames[4],
            sublocations_df=frames[5],
            locations_df=frames[6],
            priority_types_df=frames[7],
            task_categories_df=frames[8],
            categories_df=frames[9],
            comment_lookup_df=lookup,
            classifier_predictions_df=empty_classifier_predictions_df(self.spark),
        )

        row = result.select("ResultCategory", "ClassificationMethod").collect()[0]

        self.assertEqual(row.ResultCategory, "INCUMPLIMIENTO_GENERAL")
        self.assertEqual(row.ClassificationMethod, "HUMAN")

    def test_passed_task_keeps_compliance_over_human_lookup(self) -> None:
        frames = self._base_frames()
        lookup = self.spark.createDataFrame(
            [
                (
                    "cumple correctamente.",
                    1,
                    "INCUMPLIMIENTO_GENERAL",
                    "HUMAN",
                    None,
                    1.0,
                )
            ],
            "NormalizedComment string, AiResult tinyint, ResultCategory string, "
            "ClassificationMethod string, ClassifierModelVersion string, "
            "ClassifierConfidence double",
        )
        result = build_smartaudits_categories_df(
            plan_results_df=frames[0].filter("id = 2"),
            evidence_photos_df=frames[1].filter("id = 201"),
            work_plan_df=frames[2],
            tasks_df=frames[3],
            checkpoints_df=frames[4],
            sublocations_df=frames[5],
            locations_df=frames[6],
            priority_types_df=frames[7],
            task_categories_df=frames[8],
            categories_df=frames[9],
            comment_lookup_df=lookup,
            classifier_predictions_df=empty_classifier_predictions_df(self.spark),
        )

        row = result.select("ResultCategory", "ClassificationMethod").collect()[0]

        self.assertEqual(row.ResultCategory, "CUMPLIMIENTO")
        self.assertEqual(row.ClassificationMethod, "RULE")

    def test_review_queue_schema_and_grouping(self) -> None:
        evidence = self.spark.createDataFrame(
            [
                {
                    "id": 1,
                    "planResultsId": 10,
                    "resultAI": 0,
                    "commentsAI": "Comentario operativo ambiguo",
                    "createdAt": datetime(2026, 4, 1, 10, 0),
                },
                {
                    "id": 2,
                    "planResultsId": 11,
                    "resultAI": 0,
                    "commentsAI": "Comentario   operativo ambiguo",
                    "createdAt": datetime(2026, 4, 1, 11, 0),
                },
                {
                    "id": 3,
                    "planResultsId": 12,
                    "resultAI": 1,
                    "commentsAI": "Comentario operativo ambiguo",
                    "createdAt": datetime(2026, 4, 1, 12, 0),
                },
                {
                    "id": 4,
                    "planResultsId": 13,
                    "resultAI": 0,
                    "commentsAI": "   ",
                    "createdAt": datetime(2026, 4, 1, 13, 0),
                },
                {
                    "id": 5,
                    "planResultsId": 14,
                    "resultAI": 0,
                    "commentsAI": "La foto esta borrosa.",
                    "createdAt": datetime(2026, 4, 1, 14, 0),
                },
            ]
        )

        queue = build_smartaudits_comment_review_queue_df(
            evidence_photos_df=evidence,
            classifier_predictions_df=empty_classifier_predictions_df(self.spark),
        )
        rows = queue.collect()

        self.assertEqual(queue.columns, REVIEW_QUEUE_COLUMNS)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].NormalizedComment, "comentario operativo ambiguo")
        self.assertEqual(rows[0].CandidateCount, 2)
        self.assertEqual(rows[0].AiResult, 0)

    def test_source_last_changed_uses_review_ai_or_evidence_created_at(self) -> None:
        row = (
            self._result_df()
            .filter("PlanResultId = 1")
            .select("SourceLastChangedAt")
            .collect()[0]
        )

        self.assertEqual(row.SourceLastChangedAt, datetime(2026, 3, 31, 10, 32))


if __name__ == "__main__":
    unittest.main()
