import importlib
import json
import sys
import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch
from urllib.error import URLError


PROJECT_SRC = Path(__file__).resolve().parents[1] / "src"
REPOSITORY_ROOT = PROJECT_SRC.parents[2]
original_sys_path = list(sys.path)
sys.path[:] = [
    str(PROJECT_SRC),
    *[
        path
        for path in original_sys_path
        if Path(path or ".").resolve() != REPOSITORY_ROOT
    ],
]
original_dagster_module = sys.modules.pop("dagster", None)
try:
    api_module = importlib.import_module(
        "bafar.defs.resources.store_schedules_api",
    )
    asset_module = importlib.import_module(
        "bafar.defs.assets.dwh_dimStoreSchedules",
    )
    job_module = importlib.import_module(
        "bafar.defs.jobs.store_schedules_job",
    )
    schedule_module = importlib.import_module(
        "bafar.defs.schedules.store_schedules_schedule",
    )
finally:
    if original_dagster_module is not None:
        sys.modules["dagster"] = original_dagster_module
    sys.path[:] = original_sys_path


def build_store(store_id="M017"):
    schedule = []
    for day_no, day in api_module.DAY_CODES.items():
        if day == "SUN":
            schedule.append(
                {"DAY": day, "DAY_NO": day_no, "CLOSED": True, "SLOTS": []}
            )
        elif day == "MON":
            schedule.append(
                {
                    "DAY": day,
                    "DAY_NO": day_no,
                    "CLOSED": False,
                    "SLOTS": [
                        {"FROM": "06:30:00", "TO": "10:00:00"},
                        {"FROM": "11:00:00", "TO": "14:30:00"},
                    ],
                }
            )
        else:
            schedule.append(
                {
                    "DAY": day,
                    "DAY_NO": day_no,
                    "CLOSED": False,
                    "SLOTS": [{"FROM": "06:30:00", "TO": "14:30:00"}],
                }
            )
    return {
        "STORE_ID": store_id,
        "CUSTOMER_NO": store_id,
        "NAME": "Tienda de prueba",
        "OPENING_DATE": "2016-01-01",
        "CLOSING_DATE": "9999-12-31",
        "BLOCKED": False,
        "BLOCKED_FROM": "",
        "BLOCKED_TO": "",
        "RECEIVING_ID": "003",
        "SCHEDULE": schedule,
    }


def build_payload(store_id="M017", has_more=False, next_cursor=""):
    return {
        "result": {
            "RESPONSE_HORARIO": {
                "SUCCESS": True,
                "MESSAGE": "",
                "ERROR_CODE": "",
                "HTTP_STATUS": 200,
                "COUNT": 1,
                "HAS_MORE": has_more,
                "NEXT_CURSOR": next_cursor,
                "STORES": [build_store(store_id)],
            }
        }
    }


class FakeHeaders:
    def get_content_type(self):
        return "application/json"


class FakeResponse:
    status = 200
    headers = FakeHeaders()

    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def read(self):
        return json.dumps(self.payload).encode("utf-8")


class StoreSchedulesApiContractTests(unittest.TestCase):
    def setUp(self):
        self.resource = api_module.StoreSchedulesApiResource(
            url="https://api.example.test/neptune/api/RT_STORE_INF/horarios",
            user="test-user",
            password="test-password",
            timeout_seconds=30,
            page_size=100,
        )

    def test_valid_contract_normalizes_one_and_two_slots(self):
        response = self.resource.validate_page(build_payload())

        rows = self.resource.normalize_stores(response["STORES"])
        monday = rows[0]
        sunday = rows[6]

        self.assertEqual(len(rows), 7)
        self.assertEqual(monday["OpeningTime"], "06:30:00")
        self.assertEqual(monday["LunchStartTime"], "10:00:00")
        self.assertEqual(monday["LunchEndTime"], "11:00:00")
        self.assertEqual(monday["ClosingTime"], "14:30:00")
        self.assertTrue(sunday["IsClosed"])
        self.assertIsNone(sunday["OpeningTime"])
        self.assertIsNone(sunday["ClosingTime"])

    def test_contract_drift_and_incomplete_week_fail_before_staging(self):
        unknown_field = build_payload()
        unknown_field["result"]["RESPONSE_HORARIO"]["NEW_FIELD"] = "unexpected"
        missing_field = build_payload()
        del missing_field["result"]["RESPONSE_HORARIO"]["MESSAGE"]
        invalid_type = build_payload()
        invalid_type["result"]["RESPONSE_HORARIO"]["COUNT"] = "1"
        incomplete_week = build_payload()
        incomplete_week["result"]["RESPONSE_HORARIO"]["STORES"][0][
            "SCHEDULE"
        ].pop()

        for payload in (unknown_field, missing_field, invalid_type, incomplete_week):
            with self.subTest(payload=payload), self.assertRaises(ValueError):
                self.resource.validate_page(payload)

    def test_success_false_and_count_mismatch_fail(self):
        failed_response = build_payload()
        failed_response["result"]["RESPONSE_HORARIO"]["SUCCESS"] = False
        failed_response["result"]["RESPONSE_HORARIO"]["ERROR_CODE"] = (
            "PREFIX_REQUIRED"
        )
        count_mismatch = build_payload()
        count_mismatch["result"]["RESPONSE_HORARIO"]["COUNT"] = 2

        for payload in (failed_response, count_mismatch):
            with self.subTest(payload=payload), self.assertRaises(ValueError):
                self.resource.validate_page(payload)

    def test_invalid_dates_and_times_fail(self):
        invalid_date = build_payload()
        invalid_date["result"]["RESPONSE_HORARIO"]["STORES"][0][
            "OPENING_DATE"
        ] = "2026-02-30"
        invalid_time = build_payload()
        invalid_time["result"]["RESPONSE_HORARIO"]["STORES"][0]["SCHEDULE"][0][
            "SLOTS"
        ][0]["FROM"] = "25:00:00"
        invalid_blocked_dates = build_payload()
        invalid_blocked_dates["result"]["RESPONSE_HORARIO"]["STORES"][0][
            "BLOCKED_FROM"
        ] = "2026-09-20"
        invalid_blocked_dates["result"]["RESPONSE_HORARIO"]["STORES"][0][
            "BLOCKED_TO"
        ] = "2026-09-10"

        for payload in (invalid_date, invalid_time, invalid_blocked_dates):
            with self.subTest(payload=payload), self.assertRaises(ValueError):
                self.resource.validate_page(payload)

    def test_invalid_closed_and_open_day_slots_fail(self):
        closed_with_slots = build_payload()
        closed_with_slots["result"]["RESPONSE_HORARIO"]["STORES"][0][
            "SCHEDULE"
        ][6]["SLOTS"] = [{"FROM": "06:30:00", "TO": "14:30:00"}]
        open_without_slots = build_payload()
        open_without_slots["result"]["RESPONSE_HORARIO"]["STORES"][0][
            "SCHEDULE"
        ][0]["SLOTS"] = []

        for payload in (closed_with_slots, open_without_slots):
            with self.subTest(payload=payload), self.assertRaises(ValueError):
                self.resource.validate_page(payload)

    def test_more_than_two_or_overlapping_slots_fail(self):
        too_many_slots = build_payload()
        too_many_slots["result"]["RESPONSE_HORARIO"]["STORES"][0]["SCHEDULE"][0][
            "SLOTS"
        ].append({"FROM": "15:00:00", "TO": "16:00:00"})
        overlapping_slots = build_payload()
        overlapping_slots["result"]["RESPONSE_HORARIO"]["STORES"][0][
            "SCHEDULE"
        ][0]["SLOTS"][1]["FROM"] = "09:30:00"

        for payload in (too_many_slots, overlapping_slots):
            with self.subTest(payload=payload), self.assertRaises(ValueError):
                self.resource.validate_page(payload)

    def test_duplicate_stores_and_empty_cursor_fail(self):
        duplicate_stores = build_payload()
        response = duplicate_stores["result"]["RESPONSE_HORARIO"]
        response["STORES"].append(build_store())
        response["COUNT"] = 2
        empty_cursor = build_payload(has_more=True, next_cursor="")

        for payload in (duplicate_stores, empty_cursor):
            with self.subTest(payload=payload), self.assertRaises(ValueError):
                self.resource.validate_page(payload)

    def test_network_error_is_sanitized(self):
        with (
            patch.object(api_module, "urlopen", side_effect=URLError("offline")),
            self.assertRaisesRegex(RuntimeError, "request failed"),
        ):
            self.resource.fetch_page()

    def test_fetch_rejects_non_https_url(self):
        self.resource.url = "http://api.example.test/horarios"

        with (
            patch.object(api_module, "urlopen") as urlopen,
            self.assertRaisesRegex(RuntimeError, "must use HTTPS"),
        ):
            self.resource.fetch_page()

        urlopen.assert_not_called()

    def test_fetch_uses_full_catalog_closed_days_and_cursor(self):
        payload = build_payload()

        with patch.object(
            api_module,
            "urlopen",
            return_value=FakeResponse(payload),
        ) as urlopen:
            result = self.resource.fetch_page("M016")

        request = urlopen.call_args.args[0]
        body = json.loads(request.data.decode("utf-8"))
        self.assertEqual(request.method, "POST")
        self.assertEqual(
            body,
            {
                "prefix": "9999",
                "includeClosed": True,
                "pageSize": 100,
                "cursor": "M016",
            },
        )
        self.assertEqual(urlopen.call_args.kwargs["timeout"], 30)
        self.assertTrue(request.get_header("Authorization").startswith("Basic "))
        self.assertEqual(result, payload)


class StoreSchedulesAssetTests(unittest.TestCase):
    def test_asset_merges_only_after_every_page_is_valid(self):
        api = api_module.StoreSchedulesApiResource(
            url="https://api.example.test/horarios",
            user="test-user",
            password="test-password",
            timeout_seconds=30,
            page_size=100,
        )
        api.fetch_page = MagicMock(
            side_effect=[
                build_payload("M017", has_more=True, next_cursor="M017"),
                build_payload("M018"),
            ]
        )
        sqlserver = MagicMock()
        sqlserver.write_dim_store_schedules_stage.return_value = 14
        sqlserver.merge_dim_store_schedules.return_value = 14
        context = SimpleNamespace(
            resources=SimpleNamespace(
                store_schedules_api=api,
                sqlserver=sqlserver,
            ),
            run_id="run-1",
            retry_number=0,
            log=MagicMock(),
        )

        with patch.object(asset_module, "datetime", wraps=datetime) as current_datetime:
            current_datetime.utcnow.return_value = datetime(2026, 9, 10, 12)
            asset_module.ingest_dim_store_schedules_op.compute_fn.decorated_fn(context)

        self.assertEqual(api.fetch_page.call_args_list, [call(None), call("M017")])
        self.assertEqual(
            sqlserver.write_dim_store_schedules_raw_page.call_count,
            2,
        )
        staged_rows = sqlserver.write_dim_store_schedules_stage.call_args.args[1]
        self.assertEqual(len(staged_rows), 14)
        sqlserver.merge_dim_store_schedules.assert_called_once_with("run-1")
        sqlserver.clear_dim_store_schedules_stage.assert_has_calls(
            [call("run-1"), call("run-1")]
        )
        sqlserver.mark_run_completed.assert_called_once_with(
            ingestion_name="DimStoreSchedules",
            new_last_loaded_at=datetime(2026, 9, 10, 12),
            succeeded=True,
            error_message=None,
        )

    def test_page_failure_preserves_last_dwh_snapshot(self):
        api = api_module.StoreSchedulesApiResource(
            url="https://api.example.test/horarios",
            user="test-user",
            password="test-password",
            timeout_seconds=30,
            page_size=100,
        )
        api.fetch_page = MagicMock(
            side_effect=[
                build_payload("M017", has_more=True, next_cursor="M017"),
                RuntimeError("page failed"),
            ]
        )
        sqlserver = MagicMock()
        last_loaded_at = datetime(2026, 9, 9, 12)
        sqlserver.get_watermark.return_value = last_loaded_at
        context = SimpleNamespace(
            resources=SimpleNamespace(
                store_schedules_api=api,
                sqlserver=sqlserver,
            ),
            run_id="run-2",
            retry_number=3,
            log=MagicMock(),
        )

        with self.assertRaisesRegex(RuntimeError, "page failed"):
            asset_module.ingest_dim_store_schedules_op.compute_fn.decorated_fn(context)

        sqlserver.write_dim_store_schedules_stage.assert_not_called()
        sqlserver.merge_dim_store_schedules.assert_not_called()
        sqlserver.mark_run_completed.assert_called_once_with(
            ingestion_name="DimStoreSchedules",
            new_last_loaded_at=last_loaded_at,
            succeeded=False,
            error_message="page failed",
        )

    def test_duplicate_store_across_pages_preserves_last_dwh_snapshot(self):
        api = api_module.StoreSchedulesApiResource(
            url="https://api.example.test/horarios",
            user="test-user",
            password="test-password",
            timeout_seconds=30,
            page_size=100,
        )
        api.fetch_page = MagicMock(
            side_effect=[
                build_payload("M017", has_more=True, next_cursor="M017"),
                build_payload("M017"),
            ]
        )
        sqlserver = MagicMock()
        context = SimpleNamespace(
            resources=SimpleNamespace(
                store_schedules_api=api,
                sqlserver=sqlserver,
            ),
            run_id="run-3",
            retry_number=3,
            log=MagicMock(),
        )

        with self.assertRaisesRegex(ValueError, "duplicate stores across pages"):
            asset_module.ingest_dim_store_schedules_op.compute_fn.decorated_fn(context)

        sqlserver.write_dim_store_schedules_stage.assert_not_called()
        sqlserver.merge_dim_store_schedules.assert_not_called()

    def test_repeated_cursor_preserves_last_dwh_snapshot(self):
        api = api_module.StoreSchedulesApiResource(
            url="https://api.example.test/horarios",
            user="test-user",
            password="test-password",
            timeout_seconds=30,
            page_size=100,
        )
        api.fetch_page = MagicMock(
            side_effect=[
                build_payload("M017", has_more=True, next_cursor="M017"),
                build_payload("M018", has_more=True, next_cursor="M017"),
            ]
        )
        sqlserver = MagicMock()
        context = SimpleNamespace(
            resources=SimpleNamespace(
                store_schedules_api=api,
                sqlserver=sqlserver,
            ),
            run_id="run-4",
            retry_number=3,
            log=MagicMock(),
        )

        with self.assertRaisesRegex(ValueError, "cursor was repeated"):
            asset_module.ingest_dim_store_schedules_op.compute_fn.decorated_fn(context)

        sqlserver.write_dim_store_schedules_stage.assert_not_called()
        sqlserver.merge_dim_store_schedules.assert_not_called()

    def test_network_failure_on_later_page_preserves_last_dwh_snapshot(self):
        api = api_module.StoreSchedulesApiResource(
            url="https://api.example.test/horarios",
            user="test-user",
            password="test-password",
            timeout_seconds=30,
            page_size=100,
        )
        sqlserver = MagicMock()
        context = SimpleNamespace(
            resources=SimpleNamespace(
                store_schedules_api=api,
                sqlserver=sqlserver,
            ),
            run_id="run-5",
            retry_number=3,
            log=MagicMock(),
        )

        with (
            patch.object(
                api_module,
                "urlopen",
                side_effect=[
                    FakeResponse(
                        build_payload("M017", has_more=True, next_cursor="M017")
                    ),
                    URLError("offline"),
                ],
            ),
            self.assertRaisesRegex(RuntimeError, "request failed"),
        ):
            asset_module.ingest_dim_store_schedules_op.compute_fn.decorated_fn(context)

        sqlserver.write_dim_store_schedules_raw_page.assert_called_once()
        sqlserver.write_dim_store_schedules_stage.assert_not_called()
        sqlserver.merge_dim_store_schedules.assert_not_called()

    def test_normalization_stage_and_merge_failures_preserve_control_state(self):
        for failing_step in ("normalize", "stage", "merge"):
            with self.subTest(failing_step=failing_step):
                error_message = (
                    "normalization failed"
                    if failing_step == "normalize"
                    else f"{failing_step} failed"
                )
                api = api_module.StoreSchedulesApiResource(
                    url="https://api.example.test/horarios",
                    user="test-user",
                    password="test-password",
                    timeout_seconds=30,
                    page_size=100,
                )
                api.fetch_page = MagicMock(return_value=build_payload())
                sqlserver = MagicMock()
                last_loaded_at = datetime(2026, 9, 9, 12)
                sqlserver.get_watermark.return_value = last_loaded_at
                if failing_step == "normalize":
                    api.normalize_stores = MagicMock(
                        side_effect=ValueError(error_message)
                    )
                elif failing_step == "stage":
                    sqlserver.write_dim_store_schedules_stage.side_effect = RuntimeError(
                        error_message
                    )
                else:
                    sqlserver.write_dim_store_schedules_stage.return_value = 7
                    sqlserver.merge_dim_store_schedules.side_effect = RuntimeError(
                        error_message
                    )
                context = SimpleNamespace(
                    resources=SimpleNamespace(
                        store_schedules_api=api,
                        sqlserver=sqlserver,
                    ),
                    run_id="run-6",
                    retry_number=3,
                    log=MagicMock(),
                )

                with self.assertRaisesRegex(Exception, error_message):
                    asset_module.ingest_dim_store_schedules_op.compute_fn.decorated_fn(
                        context
                    )

                sqlserver.clear_dim_store_schedules_stage.assert_has_calls(
                    [call("run-6"), call("run-6")]
                )
                sqlserver.mark_run_completed.assert_called_once_with(
                    ingestion_name="DimStoreSchedules",
                    new_last_loaded_at=last_loaded_at,
                    succeeded=False,
                    error_message=error_message,
                )
                if failing_step != "merge":
                    sqlserver.merge_dim_store_schedules.assert_not_called()

    def test_job_and_schedule_contracts(self):
        self.assertEqual(job_module.store_schedules_job.name, "010_bafar_store_schedules")
        self.assertEqual(
            schedule_module.store_schedules_schedule.job_name,
            "010_bafar_store_schedules",
        )
        self.assertEqual(
            schedule_module.store_schedules_schedule.cron_schedule,
            "30 2 * * *",
        )


if __name__ == "__main__":
    unittest.main()
