from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Dict, Optional

from dagster import DagsterRunStatus, RunsFilter, asset


@asset(
    name="bridge_cleanup_old_runs",
    config_schema={"keep_days": int, "batch_size": int},
)
def bridge_cleanup_old_runs(context) -> Optional[Dict[str, str]]:
    instance = context.instance

    keep_days = int(context.op_config.get("keep_days", 14))
    batch_size = int(context.op_config.get("batch_size", 200))
    cutoff_dt = datetime.now(timezone.utc) - timedelta(days=keep_days)

    statuses = [
        DagsterRunStatus.SUCCESS,
        DagsterRunStatus.FAILURE,
        DagsterRunStatus.CANCELED,
    ]

    deleted = 0
    scanned = 0

    for status in statuses:
        cursor = None

        while True:
            records = instance.get_run_records(
                filters=RunsFilter(statuses=[status]),
                limit=batch_size,
                cursor=cursor,
            )
            if not records:
                break

            cursor = records[-1].dagster_run.run_id

            for record in records:
                scanned += 1
                run_id = record.dagster_run.run_id
                timestamp = record.update_timestamp or record.create_timestamp
                if not timestamp:
                    continue

                if isinstance(timestamp, (int, float)):
                    run_dt = datetime.fromtimestamp(float(timestamp), tz=timezone.utc)
                else:
                    run_dt = timestamp
                    if run_dt.tzinfo is None:
                        run_dt = run_dt.replace(tzinfo=timezone.utc)

                if run_dt < cutoff_dt:
                    try:
                        instance.delete_run(run_id)
                        deleted += 1
                    except Exception as ex:
                        context.log.warning("Failed to delete run %s: %s", run_id, ex)

    context.log.warning(
        "Bridge Dagster cleanup complete. keep_days=%s batch_size=%s scanned=%s deleted=%s cutoff=%s",
        keep_days,
        batch_size,
        scanned,
        deleted,
        cutoff_dt.isoformat(),
    )

    return {
        "keep_days": str(keep_days),
        "batch_size": str(batch_size),
        "cutoff": cutoff_dt.isoformat(),
        "scanned": str(scanned),
        "deleted": str(deleted),
    }
