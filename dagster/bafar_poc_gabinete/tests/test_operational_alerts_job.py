import copy
import importlib
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock

import pytest


PROJECT_SRC = Path(__file__).resolve().parents[1] / "src"
REPOSITORY_ROOT = PROJECT_SRC.parents[2]
original_sys_path = list(sys.path)
sys.path[:] = [str(PROJECT_SRC), *[
    path for path in original_sys_path if Path(path or ".").resolve() != REPOSITORY_ROOT
]]
original_dagster_module = sys.modules.pop("dagster", None)
try:
    import dagster as dg
finally:
    if original_dagster_module is not None:
        sys.modules["dagster"] = original_dagster_module
    sys.path[:] = original_sys_path


@pytest.fixture
def operational(monkeypatch):
    tenant = "bafar_poc_gabinete"
    sensor = importlib.import_module(f"{tenant}.defs.sensors.operational_alerts_sensor")
    asset = importlib.import_module(f"{tenant}.defs.assets.operational_alerts")
    scan_failures = sensor._failed_run_assets
    now = datetime.now(timezone.utc)
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    config = {
        "version_token": "config-1", "sensor_lookback_hours": 1,
        "dagster_failure_lookback_minutes": 120, "dagster_failure_batch_size": 200,
        "rules": {(kind, None): {
            "alert_type": kind, "alert_type_id": f"{kind}-id", "alert_rule_id": f"{kind}-rule",
            "alert_config_id": "config-id", "rule_version": b"1", "is_enabled": True,
            "severity_id": "warning-id", "severity": "WARNING", "evaluation_interval_seconds": 30,
            "no_readings_minutes": 20, "source_max_age_minutes": 60, "expected_interval_minutes": 60,
        } for kind in (*sensor._OPERATIONAL_TYPES, "dagster_run_failure")},
    }
    inventory = {"sensors": [{"entity_key": "sensor-1"}], "ingestions": [{"entity_key": "ingestion-1"}], "incidents": []}
    statuses = [{
        "sensor_id": "sensor-1", "observed_at": now - timedelta(minutes=1),
        "last_reading_at": now - timedelta(minutes=25), "minutes_without_readings": 25,
        "modified_at": now - timedelta(minutes=1),
    }]
    ingestions = [{
        "ingestion_name": "ingestion-1", "last_run_status": "FAILED",
        "last_run_started_at": now - timedelta(minutes=20),
        "last_run_finished_at": now - timedelta(minutes=10), "last_run_error": "Synthetic failure",
    }]
    incidents = {}
    loader = Mock(return_value=(config, None))
    detected, cleared, failures = Mock(return_value=True), Mock(return_value=True), Mock(return_value=[])
    monkeypatch.setattr(sensor, "_load_active_config", loader)
    monkeypatch.setattr(asset, "_load_active_config", loader)
    monkeypatch.setattr(sensor, "_rows", Mock(side_effect=lambda _: (
        inventory["sensors"] if "factRedingsAudits" in cursor.execute.call_args.args[0]
        else inventory["ingestions"] if "IngestionControl" in cursor.execute.call_args.args[0]
        else inventory["incidents"]
    )))
    monkeypatch.setattr(asset, "_load_active_incidents", Mock(return_value=incidents))
    monkeypatch.setattr(asset, "_sensor_statuses", Mock(return_value=statuses))
    monkeypatch.setattr(asset, "_sensor_dimensions", Mock(return_value={}))
    monkeypatch.setattr(asset, "_ingestions", Mock(return_value=ingestions))
    for module in (asset, sensor):
        monkeypatch.setattr(module, "_record_detected", detected)
        monkeypatch.setattr(module, "_record_cleared", cleared)
        monkeypatch.setattr(module, "_failed_run_assets", failures)
    context = SimpleNamespace(
        cursor=None, update_cursor=Mock(), instance=Mock(), run_id="evaluation-run",
        resources=SimpleNamespace(sqlserver=SimpleNamespace(_get_connection=Mock(return_value=connection))),
        op_config={"alert_type": "connection_lost", "entity_key": "sensor-1", "evaluation_key": "sensor-1",
                   "source_run_id": "", "incident_id": "", "config_version": "config-1", "rule_version": "31"},
    )
    context.instance.get_runs.return_value = []
    return SimpleNamespace(
        tenant=tenant, sensor=sensor, asset=asset, context=context, connection=connection, config=config,
        loader=loader, statuses=statuses, ingestions=ingestions, incidents=incidents, inventory=inventory,
        detected=detected, cleared=cleared, failures=failures, now=now, scan_failures=scan_failures,
    )


def test_registration_preserves_tenant_resources_and_sensor_status():
    tenant = "bafar_poc_gabinete"
    case = SimpleNamespace(tenant=tenant,
                           sensor=importlib.import_module(f"{tenant}.defs.sensors.operational_alerts_sensor"),
                           asset=importlib.import_module(f"{tenant}.defs.assets.operational_alerts"))
    definitions = importlib.import_module(f"{case.tenant}.definitions").defs()
    job = definitions.resolve_job_def("operational_alerts_job")
    assert job.has_node_named(f"{case.tenant}__operational_alerts")
    assert job.tags["user_code"] == case.tenant and job.tags["dagster/max_retries"] == "0"
    expected = dg.DefaultSensorStatus.STOPPED
    assert case.sensor.operational_alerts_sensor.default_status == expected
    assert case.asset.operational_alerts.required_resource_keys == {"sqlserver", "io_manager"}


def test_sensor_dispatches_one_entity_without_evaluating_or_writing(operational):
    case = operational
    request = case.sensor.operational_alerts_sensor._raw_fn(case.context)
    assert isinstance(request, dg.RunRequest)
    assert request.run_config["ops"][f"{case.tenant}__operational_alerts"]["config"] == case.context.op_config
    state = json.loads(case.context.update_cursor.call_args.args[0])
    assert len(state["pending"]) == 4
    assert state["in_flight"]["run_key"] == request.run_key
    case.asset._sensor_statuses.assert_not_called()
    case.asset._ingestions.assert_not_called()
    case.detected.assert_not_called()
    case.cleared.assert_not_called()
    assert all("INSERT" not in call.args[0] and "UPDATE" not in call.args[0] for call in case.connection.cursor.return_value.__enter__.return_value.execute.call_args_list)
    case.connection.close.assert_called_once()


@pytest.mark.parametrize("status", ["QUEUED", "STARTING", "STARTED", "CANCELING"])
def test_active_run_blocks_new_dispatch_without_reading_sources(operational, status):
    case = operational
    case.sensor.operational_alerts_sensor._raw_fn(case.context)
    case.context.cursor = case.context.update_cursor.call_args.args[0]
    case.context.instance.get_runs.return_value = [SimpleNamespace(is_finished=False, status=getattr(dg.DagsterRunStatus, status))]
    case.loader.reset_mock()
    assert isinstance(case.sensor.operational_alerts_sensor._raw_fn(case.context), dg.SkipReason)
    case.loader.assert_not_called()


def test_missing_run_reuses_request_and_tenant_filter(operational):
    case = operational
    first = case.sensor.operational_alerts_sensor._raw_fn(case.context)
    case.context.cursor = case.context.update_cursor.call_args.args[0]
    assert case.sensor.operational_alerts_sensor._raw_fn(case.context) == first
    filters = case.context.instance.get_runs.call_args.kwargs["filters"]
    assert filters.tags["user_code"] == case.tenant and filters.tags["dagster/run_key"] == first.run_key


def test_success_advances_to_other_due_entities_and_honors_existing_checks(operational):
    case = operational
    case.context.cursor = json.dumps({"checks": {"connection_lost::sensor-1": {"version": "config-1:31", "at": case.now.timestamp()}}})
    request = case.sensor.operational_alerts_sensor._raw_fn(case.context)
    assert request.tags["alert_type"] == "late_readings"
    case.context.cursor = case.context.update_cursor.call_args.args[0]
    case.context.instance.get_runs.return_value = [SimpleNamespace(is_finished=True, status=dg.DagsterRunStatus.SUCCESS)]
    second = case.sensor.operational_alerts_sensor._raw_fn(case.context)
    assert second.tags == {"alert_type": "pipeline_failure", "entity_key": "ingestion-1"}
    assert second.run_key != request.run_key


@pytest.mark.parametrize("status", ["FAILURE", "CANCELED"])
def test_failed_request_survives_disappearing_source_and_retries_after_interval(operational, status):
    case = operational
    case.inventory["sensors"] = []
    case.inventory["ingestions"] = []
    case.failures.return_value = [{"asset_key": "affected-asset", "run_id": "failed-source-run"}]
    first = case.sensor.operational_alerts_sensor._raw_fn(case.context)
    case.context.cursor = case.context.update_cursor.call_args.args[0]
    case.context.instance.get_runs.return_value = [SimpleNamespace(is_finished=True, status=getattr(dg.DagsterRunStatus, status))]
    case.failures.return_value = []
    assert isinstance(case.sensor.operational_alerts_sensor._raw_fn(case.context), dg.SkipReason)
    state = json.loads(case.context.update_cursor.call_args.args[0])
    assert len(state["pending"]) == 1
    for check in state["checks"].values():
        check["at"] = 0
    case.context.cursor = json.dumps(state)
    retry = case.sensor.operational_alerts_sensor._raw_fn(case.context)
    assert retry.run_key != first.run_key and retry.run_config == first.run_config


@pytest.mark.parametrize("kind", ["connection_lost", "late_readings"])
@pytest.mark.parametrize("evidence", ["valid", "missing", "stale", "future", "zero", "existing", "deduplicated"])
def test_sensor_alert_evidence_and_persistence_outcomes(operational, kind, evidence):
    case = operational
    case.context.op_config["alert_type"] = kind
    if evidence == "missing":
        case.statuses[0]["last_reading_at"] = None
    elif evidence == "stale":
        case.statuses[0]["modified_at"] = case.now - timedelta(hours=2)
    elif evidence == "future":
        case.statuses[0]["observed_at"] = case.now + timedelta(hours=1)
    elif evidence == "zero":
        case.statuses[0]["minutes_without_readings"] = 0
    elif evidence == "existing":
        case.incidents[(kind, "sensor-1")] = {"no_readings_minutes": 20, "source_max_age_minutes": 60}
    elif evidence == "deduplicated":
        case.detected.return_value = False
    result = case.asset.operational_alerts.op.compute_fn.decorated_fn(case.context)
    expected = {"valid": "detected", "missing": "incomplete_evidence", "stale": "stale_or_future_evidence",
                "future": "stale_or_future_evidence", "zero": "condition_absent", "existing": "condition_active", "deduplicated": "deduplicated"}
    assert result.metadata["outcome"] == expected[evidence]
    assert result.metadata["detected_count"] == (1 if evidence == "valid" else 0)
    assert case.detected.called == (evidence in ("valid", "deduplicated"))
    assert not case.cleared.called
    assert result.check_results[0].passed == (evidence not in ("missing", "stale", "future"))


@pytest.mark.parametrize("kind", ["pipeline_failure", "pipeline_stale"])
@pytest.mark.parametrize("status,finished_minutes,incident_minutes,detects,clears", [
    ("FAILED", 10, None, True, False),
    ("RUNNING", None, None, False, False),
    ("SUCCESS", 10, None, False, False),
    ("SUCCESS", 90, None, True, False),
    ("SUCCESS", 10, 120, False, True),
    ("SUCCESS", 90, 120, True, True),
    ("SUCCESS", 120, 90, False, False),
    ("SUCCESS", -60, 120, False, False),
])
def test_pipeline_alerts_require_real_evidence_and_preserve_reopening(operational, kind, status, finished_minutes, incident_minutes, detects, clears):
    case = operational
    case.context.op_config.update(alert_type=kind, entity_key="ingestion-1", evaluation_key="ingestion-1")
    case.ingestions[0].update(last_run_status=status, last_run_finished_at=(case.now - timedelta(minutes=finished_minutes) if finished_minutes is not None else None))
    if incident_minutes is not None:
        case.incidents[(kind, "ingestion-1")] = {"alert_id": "old-incident", "detected_at": case.now - timedelta(minutes=incident_minutes)}
    result = case.asset.operational_alerts.op.compute_fn.decorated_fn(case.context)
    expected_detect = detects and (status == "FAILED" if kind == "pipeline_failure" else status == "SUCCESS")
    assert result.metadata["detected_count"] == int(expected_detect)
    assert result.metadata["cleared_count"] == int(clears)
    assert case.detected.called == expected_detect and case.cleared.called == clears
    if expected_detect and clears:
        assert result.metadata["outcome"] == "cleared_and_detected"
    if finished_minutes == -60:
        assert result.metadata["outcome"] == "future_evidence"
        assert not result.check_results[0].passed


@pytest.mark.parametrize("change", ["disabled", "version", "rule", "rule_during_read"])
def test_configuration_changes_prevent_writes(operational, change):
    case = operational
    changed = copy.deepcopy(case.config)
    if change == "disabled":
        changed["rules"][("connection_lost", None)]["is_enabled"] = False
    elif change == "version":
        changed["version_token"] = "config-2"
    else:
        changed["rules"][("connection_lost", None)]["rule_version"] = b"2"
    case.loader.side_effect = [(case.config, None), (changed, None)] if change == "rule_during_read" else [(changed, None)]
    with pytest.raises(dg.Failure):
        case.asset.operational_alerts.op.compute_fn.decorated_fn(case.context)
    case.detected.assert_not_called()
    case.cleared.assert_not_called()


@pytest.mark.parametrize("failure", [None, "read", "write", "commit"])
def test_actual_job_materialization_and_cursor_acknowledgement(operational, failure):
    case = operational
    request = case.sensor.operational_alerts_sensor._raw_fn(case.context)
    case.context.cursor = case.context.update_cursor.call_args.args[0]
    case.inventory["sensors"] = []
    case.inventory["ingestions"] = []
    if failure == "read":
        case.asset._sensor_statuses.side_effect = RuntimeError("Synthetic read failure")
    elif failure == "write":
        case.detected.side_effect = RuntimeError("Synthetic write failure")
    elif failure == "commit":
        case.connection.__exit__.side_effect = RuntimeError("Synthetic commit failure")
    definitions = dg.Definitions(
        assets=[case.asset.operational_alerts], jobs=[case.sensor.operational_alerts_job],
        resources={"sqlserver": dg.ResourceDefinition.hardcoded_resource(case.context.resources.sqlserver)},
    )
    job = definitions.resolve_job_def("operational_alerts_job")
    with dg.DagsterInstance.ephemeral() as instance:
        instance.create_run_for_job(job, run_config=request.run_config, status=dg.DagsterRunStatus.SUCCESS,
                                    tags={"user_code": "other", "dagster/run_key": request.run_key, "dagster/sensor_name": "operational_alerts_sensor"})
        case.context.instance = instance
        assert case.sensor.operational_alerts_sensor._raw_fn(case.context) == request
        result = job.execute_in_process(instance=instance, run_config=request.run_config, raise_on_error=False,
                                        tags={"dagster/run_key": request.run_key, "dagster/sensor_name": "operational_alerts_sensor"})
        assert result.success == (failure is None)
        materializations = result.get_asset_materialization_events()
        assert len(materializations) == (1 if failure is None else 0)
        if failure is None:
            metadata = materializations[0].event_specific_data.materialization.metadata
            assert metadata["detected_count"].value == 1
            assert metadata["evaluation"].value["evaluation_run_id"] == result.run_id
            assert result.get_asset_check_evaluations()[0].passed
        case.sensor.operational_alerts_sensor._raw_fn(case.context)
        state = json.loads(case.context.update_cursor.call_args.args[0])
        assert ("connection_lost::sensor-1" in state["pending"]) == (failure is not None)


@pytest.mark.parametrize("own_job", [False, True])
def test_failure_lookup_preserves_queued_run_outside_lookback_and_avoids_recursion(operational, own_job):
    case = operational
    instance = Mock()
    run = SimpleNamespace(
        run_id="failed-run", job_name="operational_alerts_job" if own_job else "source_job",
        execution_plan_snapshot_id="snapshot", remote_job_origin=SimpleNamespace(repository_origin=SimpleNamespace(
            code_location_origin=SimpleNamespace(location_name=case.tenant))),
    )
    instance.get_run_records.return_value = [SimpleNamespace(dagster_run=run)]
    instance.get_execution_plan_snapshot.return_value = SimpleNamespace(
        step_keys_to_execute=["step"], step_deps={"step": []},
        steps=[SimpleNamespace(key="step", outputs=[SimpleNamespace(entity_key=dg.AssetKey([case.tenant, "target"]))])],
    )
    instance.get_records_for_run.side_effect = lambda *args, **kwargs: SimpleNamespace(records=([] if kwargs["of_type"] == dg.DagsterEventType.ASSET_MATERIALIZATION else [SimpleNamespace(
        event_log_entry=SimpleNamespace(step_key="step", timestamp=(case.now - timedelta(days=2)).timestamp()))]))
    failures = case.scan_failures(SimpleNamespace(instance=instance), 120, 200, case.now, run_id="failed-run")
    assert len(failures) == (0 if own_job else 1)
    filters = instance.get_run_records.call_args.kwargs["filters"]
    assert filters.run_ids == ["failed-run"] and filters.updated_after is None


def test_dagster_job_targets_only_requested_failure(operational):
    case = operational
    case.context.op_config.update(alert_type="dagster_run_failure", entity_key="asset-1", evaluation_key="source-run::asset-1", source_run_id="source-run")
    case.failures.return_value = [dict(run_id="source-run", asset_key=key, job_name="source-job", step_key="failed-step", occurred_at=case.now - timedelta(minutes=10)) for key in ("asset-1", "asset-2")]
    result = case.asset.operational_alerts.op.compute_fn.decorated_fn(case.context)
    case.failures.assert_called_once()
    assert case.failures.call_args.kwargs["run_id"] == "source-run"
    case.detected.assert_called_once()
    assert case.detected.call_args.args[3:5] == ("source-run", "asset-1")
    assert result.metadata["detected_count"] == 1


def test_recovery_job_targets_single_incident_and_records_evidence(operational):
    case = operational
    key = f"{case.tenant}/asset"
    case.context.op_config.update(alert_type="dagster_run_failure", entity_key=key, evaluation_key="recovery::incident-1", incident_id="incident-1")
    case.inventory["incidents"] = [dict(alert_id=f"incident-{i}", entity_id=f"source-{i}", entity_key=key, detected_at=case.now - timedelta(days=2)) for i in (1, 2)]
    case.context.instance.fetch_materializations.return_value = SimpleNamespace(records=[SimpleNamespace(
        run_id="recovery-run", timestamp=(case.now - timedelta(days=1)).timestamp(), storage_id=42)], has_more=False)
    case.context.instance.get_run_by_id.return_value = SimpleNamespace(remote_job_origin=SimpleNamespace(repository_origin=SimpleNamespace(code_location_origin=SimpleNamespace(location_name=case.tenant))))
    result = case.asset.operational_alerts.op.compute_fn.decorated_fn(case.context)
    assert result.metadata["cleared_count"] == 1
    case.cleared.assert_called_once()
    assert case.cleared.call_args.args[1]["alert_id"] == "incident-1"
    evidence = result.metadata["evaluation"].value["recoveries"][0]["evidence"]
    assert evidence["materialization_storage_id"] == 42 and evidence["recovery_run_id"] == "recovery-run"
