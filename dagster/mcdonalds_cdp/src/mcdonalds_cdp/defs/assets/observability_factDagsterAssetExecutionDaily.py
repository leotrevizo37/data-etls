import json
from datetime import datetime, timedelta, timezone
from time import perf_counter

from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    RetryPolicy,
    asset,
)
from sqlalchemy import text

from ..resources import SqlServerResource


FACT_DAGSTER_ASSET_EXECUTION_DAILY_INGESTION_NAME = (
    "factDagsterAssetExecutionDaily"
)
FACT_DAGSTER_ASSET_EXECUTION_DAILY_RETRY_POLICY = RetryPolicy(
    max_retries=3,
    delay=60,
)
DAGSTER_ASSET_EXECUTION_DAILY_SQL = """
WITH planned_assets AS (
    SELECT
        run_id,
        step_key,
        asset_key,
        partition,
        MIN(timestamp) AS planned_at
    FROM event_logs
    WHERE
        dagster_event_type = 'ASSET_MATERIALIZATION_PLANNED'
        AND asset_key IS NOT NULL
        AND asset_key::jsonb ->> 0 = :user_code
        AND timestamp < CAST(:to_utc AS TIMESTAMP)
    GROUP BY run_id, step_key, asset_key, partition
),
planned_steps AS (
    SELECT DISTINCT run_id, step_key
    FROM planned_assets
),
step_starts AS (
    SELECT
        event_logs.run_id,
        event_logs.step_key,
        MIN(event_logs.timestamp) AS started_at
    FROM event_logs
    INNER JOIN planned_steps
        ON planned_steps.run_id = event_logs.run_id
        AND planned_steps.step_key = event_logs.step_key
    WHERE
        event_logs.dagster_event_type IN ('STEP_START', 'STEP_RESTARTED')
        AND event_logs.timestamp < CAST(:to_utc AS TIMESTAMP)
    GROUP BY event_logs.run_id, event_logs.step_key
),
materializations AS (
    SELECT
        event_logs.run_id,
        event_logs.step_key,
        event_logs.asset_key,
        event_logs.partition,
        MIN(event_logs.timestamp) AS materialized_at
    FROM event_logs
    INNER JOIN planned_assets
        ON planned_assets.run_id = event_logs.run_id
        AND planned_assets.step_key = event_logs.step_key
        AND planned_assets.asset_key = event_logs.asset_key
        AND planned_assets.partition IS NOT DISTINCT FROM event_logs.partition
    WHERE
        event_logs.dagster_event_type = 'ASSET_MATERIALIZATION'
        AND event_logs.timestamp < CAST(:to_utc AS TIMESTAMP)
    GROUP BY
        event_logs.run_id,
        event_logs.step_key,
        event_logs.asset_key,
        event_logs.partition
),
materialization_metadata_values AS (
    SELECT
        event_logs.run_id,
        event_logs.step_key,
        event_logs.asset_key,
        event_logs.partition,
        metadata_entry ->> 'label' AS metadata_key,
        NULLIF(
            metadata_entry -> 'entry_data' ->> 'value',
            ''
        )::BIGINT AS metadata_value
    FROM event_logs
    INNER JOIN materializations
        ON materializations.run_id = event_logs.run_id
        AND materializations.step_key = event_logs.step_key
        AND materializations.asset_key = event_logs.asset_key
        AND materializations.partition IS NOT DISTINCT FROM event_logs.partition
    CROSS JOIN LATERAL jsonb_array_elements(
        COALESCE(
            event_logs.event::jsonb #> '{dagster_event,event_specific_data,materialization,metadata_entries}',
            '[]'::jsonb
        )
    ) AS metadata_entry
    WHERE
        event_logs.dagster_event_type = 'ASSET_MATERIALIZATION'
        AND metadata_entry ->> 'label' IN (
            'row_count',
            'staged_row_count',
            'staged_rows',
            'merged_rows',
            'evaluated_rows',
            'affected_rows',
            'review_queue_rows',
            'review_queue_merged_rows',
            'detail_row_count'
        )
),
materialization_row_metadata AS (
    SELECT
        run_id,
        step_key,
        asset_key,
        partition,
        MAX(metadata_value) FILTER (
            WHERE metadata_key = 'row_count'
        ) AS row_count,
        MAX(metadata_value) FILTER (
            WHERE metadata_key = 'staged_row_count'
        ) AS staged_row_count,
        MAX(metadata_value) FILTER (
            WHERE metadata_key = 'staged_rows'
        ) AS staged_rows,
        MAX(metadata_value) FILTER (
            WHERE metadata_key = 'merged_rows'
        ) AS merged_rows,
        MAX(metadata_value) FILTER (
            WHERE metadata_key = 'evaluated_rows'
        ) AS evaluated_rows,
        MAX(metadata_value) FILTER (
            WHERE metadata_key = 'affected_rows'
        ) AS affected_rows,
        MAX(metadata_value) FILTER (
            WHERE metadata_key = 'review_queue_rows'
        ) AS review_queue_rows,
        MAX(metadata_value) FILTER (
            WHERE metadata_key = 'review_queue_merged_rows'
        ) AS review_queue_merged_rows,
        MAX(metadata_value) FILTER (
            WHERE metadata_key = 'detail_row_count'
        ) AS detail_row_count
    FROM materialization_metadata_values
    GROUP BY run_id, step_key, asset_key, partition
),
asset_check_row_metadata AS (
    SELECT
        asset_check_executions.run_id,
        asset_check_executions.asset_key,
        MAX(
            NULLIF(check_metadata.value ->> 'value', '')::BIGINT
        ) AS row_count
    FROM asset_check_executions
    CROSS JOIN LATERAL jsonb_each(
        COALESCE(
            asset_check_executions.evaluation_event::jsonb #> '{dagster_event,event_specific_data,metadata}',
            '{}'::jsonb
        )
    ) AS check_metadata
    WHERE
        asset_check_executions.asset_key IS NOT NULL
        AND asset_check_executions.asset_key::jsonb ->> 0 = :user_code
        AND asset_check_executions.evaluation_event IS NOT NULL
        AND asset_check_executions.evaluation_event_timestamp
            < CAST(:to_utc AS TIMESTAMP)
        AND check_metadata.key = 'row_count'
    GROUP BY
        asset_check_executions.run_id,
        asset_check_executions.asset_key
),
run_asset_materialization_metadata AS (
    SELECT
        materializations.run_id,
        materializations.asset_key,
        CAST(materializations.materialized_at AS DATE) AS metric_date,
        SUM(materialization_row_metadata.row_count) AS row_count,
        SUM(
            materialization_row_metadata.staged_row_count
        ) AS staged_row_count,
        SUM(materialization_row_metadata.staged_rows) AS staged_rows,
        SUM(materialization_row_metadata.merged_rows) AS merged_rows,
        SUM(
            materialization_row_metadata.evaluated_rows
        ) AS evaluated_rows,
        SUM(materialization_row_metadata.affected_rows) AS affected_rows,
        SUM(
            materialization_row_metadata.review_queue_rows
        ) AS review_queue_rows,
        SUM(
            materialization_row_metadata.review_queue_merged_rows
        ) AS review_queue_merged_rows,
        SUM(
            materialization_row_metadata.detail_row_count
        ) AS detail_row_count
    FROM materializations
    LEFT JOIN materialization_row_metadata
        ON materialization_row_metadata.run_id = materializations.run_id
        AND materialization_row_metadata.step_key = materializations.step_key
        AND materialization_row_metadata.asset_key = materializations.asset_key
        AND materialization_row_metadata.partition
            IS NOT DISTINCT FROM materializations.partition
    WHERE
        materializations.materialized_at < CAST(:to_utc AS TIMESTAMP)
        AND (
            :from_utc IS NULL
            OR materializations.materialized_at
                >= CAST(:from_utc AS TIMESTAMP)
        )
    GROUP BY
        materializations.run_id,
        materializations.asset_key,
        CAST(materializations.materialized_at AS DATE)
),
materialization_row_changes AS (
    SELECT
        run_asset_materialization_metadata.asset_key,
        run_asset_materialization_metadata.metric_date,
        CASE
            WHEN
                run_asset_materialization_metadata.staged_row_count
                    IS NOT NULL
                OR run_asset_materialization_metadata.staged_rows IS NOT NULL
                OR run_asset_materialization_metadata.evaluated_rows
                    IS NOT NULL
                OR run_asset_materialization_metadata.review_queue_rows
                    IS NOT NULL
                OR run_asset_materialization_metadata.detail_row_count
                    IS NOT NULL
                OR (
                    (
                        run_asset_materialization_metadata.merged_rows
                            IS NOT NULL
                        OR run_asset_materialization_metadata.asset_key::jsonb
                            ->> -1 IN (
                                'dwh_factlecturas',
                                'dwh_factReadingsMeasurement'
                            )
                    )
                    AND COALESCE(
                        run_asset_materialization_metadata.row_count,
                        asset_check_row_metadata.row_count
                    ) IS NOT NULL
                )
                THEN CAST(
                    COALESCE(
                        run_asset_materialization_metadata.staged_row_count,
                        run_asset_materialization_metadata.staged_rows,
                        run_asset_materialization_metadata.evaluated_rows,
                        COALESCE(
                            run_asset_materialization_metadata.row_count,
                            asset_check_row_metadata.row_count
                        ),
                        0
                    )
                    + COALESCE(
                        run_asset_materialization_metadata.review_queue_rows,
                        0
                    )
                    + COALESCE(
                        run_asset_materialization_metadata.detail_row_count,
                        0
                    )
                    AS BIGINT
                )
        END AS inserted_row_count,
        CASE
            WHEN
                run_asset_materialization_metadata.merged_rows IS NOT NULL
                OR run_asset_materialization_metadata.affected_rows IS NOT NULL
                OR run_asset_materialization_metadata.review_queue_merged_rows
                    IS NOT NULL
                OR (
                    (
                        run_asset_materialization_metadata.staged_row_count
                            IS NOT NULL
                        OR run_asset_materialization_metadata.staged_rows
                            IS NOT NULL
                        OR run_asset_materialization_metadata.asset_key::jsonb
                            ->> -1 IN (
                                'dwh_factReadingsMeasurement',
                                'dwh_factsensorevents',
                                'observability_factredingsaudits',
                                'observability_facturlavailabilitydaily',
                                'observability_factdagsterassetexecutiondaily'
                            )
                    )
                    AND COALESCE(
                        run_asset_materialization_metadata.row_count,
                        asset_check_row_metadata.row_count
                    ) IS NOT NULL
                )
                THEN CAST(
                    COALESCE(
                        run_asset_materialization_metadata.merged_rows,
                        run_asset_materialization_metadata.affected_rows,
                        COALESCE(
                            run_asset_materialization_metadata.row_count,
                            asset_check_row_metadata.row_count
                        ),
                        0
                    )
                    + COALESCE(
                        run_asset_materialization_metadata.review_queue_merged_rows,
                        0
                    )
                    AS BIGINT
                )
        END AS merged_row_count
    FROM run_asset_materialization_metadata
    LEFT JOIN asset_check_row_metadata
        ON asset_check_row_metadata.run_id
            = run_asset_materialization_metadata.run_id
        AND asset_check_row_metadata.asset_key
            = run_asset_materialization_metadata.asset_key
),
asset_row_aggregates AS (
    SELECT
        asset_key,
        metric_date,
        SUM(inserted_row_count)::BIGINT AS inserted_row_count,
        SUM(merged_row_count)::BIGINT AS merged_row_count
    FROM materialization_row_changes
    GROUP BY asset_key, metric_date
),
step_failures AS (
    SELECT
        event_logs.run_id,
        event_logs.step_key,
        MAX(event_logs.timestamp) AS failed_at
    FROM event_logs
    INNER JOIN planned_steps
        ON planned_steps.run_id = event_logs.run_id
        AND planned_steps.step_key = event_logs.step_key
    WHERE
        event_logs.dagster_event_type = 'STEP_FAILURE'
        AND event_logs.timestamp < CAST(:to_utc AS TIMESTAMP)
    GROUP BY event_logs.run_id, event_logs.step_key
),
terminal_attempts AS (
    SELECT
        planned_assets.run_id,
        planned_assets.step_key,
        planned_assets.asset_key,
        planned_assets.partition,
        planned_assets.planned_at,
        step_starts.started_at,
        materializations.materialized_at,
        step_failures.failed_at,
        COALESCE(
            materializations.materialized_at,
            step_failures.failed_at
        ) AS terminal_at,
        CASE
            WHEN runs.start_time IS NULL THEN NULL
            ELSE GREATEST(
                runs.start_time
                - EXTRACT(EPOCH FROM runs.create_timestamp),
                0.0
            )
        END AS queued_seconds
    FROM planned_assets
    INNER JOIN runs
        ON runs.run_id = planned_assets.run_id
    LEFT JOIN step_starts
        ON step_starts.run_id = planned_assets.run_id
        AND step_starts.step_key = planned_assets.step_key
    LEFT JOIN materializations
        ON materializations.run_id = planned_assets.run_id
        AND materializations.step_key = planned_assets.step_key
        AND materializations.asset_key = planned_assets.asset_key
        AND materializations.partition IS NOT DISTINCT FROM planned_assets.partition
    LEFT JOIN step_failures
        ON step_failures.run_id = planned_assets.run_id
        AND step_failures.step_key = planned_assets.step_key
),
run_asset_steps AS (
    SELECT DISTINCT run_id, step_key, asset_key
    FROM planned_assets
    UNION
    SELECT DISTINCT
        run_id,
        evaluation_event::jsonb ->> 'step_key' AS step_key,
        asset_key
    FROM asset_check_executions
    WHERE
        asset_key IS NOT NULL
        AND asset_key::jsonb ->> 0 = :user_code
        AND evaluation_event IS NOT NULL
        AND evaluation_event::jsonb ->> 'step_key' IS NOT NULL
        AND evaluation_event_timestamp < CAST(:to_utc AS TIMESTAMP)
),
run_asset_nodes AS (
    SELECT DISTINCT
        run_id,
        split_part(step_key, '.', 1) AS asset_node,
        asset_key
    FROM run_asset_steps
),
run_assets AS (
    SELECT DISTINCT run_id, asset_key
    FROM run_asset_nodes
),
run_step_starts AS (
    SELECT
        event_logs.run_id,
        event_logs.step_key,
        MIN(event_logs.timestamp) AS started_at
    FROM event_logs
    INNER JOIN (
        SELECT DISTINCT run_id
        FROM run_assets
    ) AS asset_runs
        ON asset_runs.run_id = event_logs.run_id
    WHERE
        event_logs.dagster_event_type IN (
            'STEP_START',
            'STEP_RESTARTED'
        )
        AND event_logs.timestamp < CAST(:to_utc AS TIMESTAMP)
    GROUP BY event_logs.run_id, event_logs.step_key
),
run_step_ends AS (
    SELECT
        event_logs.run_id,
        event_logs.step_key,
        MAX(event_logs.timestamp) AS ended_at
    FROM event_logs
    INNER JOIN (
        SELECT DISTINCT run_id
        FROM run_assets
    ) AS asset_runs
        ON asset_runs.run_id = event_logs.run_id
    WHERE
        event_logs.dagster_event_type IN (
            'STEP_SUCCESS',
            'STEP_FAILURE'
        )
        AND event_logs.timestamp < CAST(:to_utc AS TIMESTAMP)
    GROUP BY event_logs.run_id, event_logs.step_key
),
run_node_costs AS (
    SELECT
        run_step_starts.run_id,
        split_part(run_step_starts.step_key, '.', 1)
            AS asset_node,
        SUM(
            GREATEST(
                EXTRACT(
                    EPOCH FROM (
                        run_step_ends.ended_at
                        - run_step_starts.started_at
                    )
                ),
                0.0
            )
        ) AS node_step_seconds
    FROM run_step_starts
    INNER JOIN run_step_ends
        ON run_step_ends.run_id = run_step_starts.run_id
        AND run_step_ends.step_key = run_step_starts.step_key
    WHERE run_step_ends.ended_at > run_step_starts.started_at
    GROUP BY
        run_step_starts.run_id,
        split_part(run_step_starts.step_key, '.', 1)
),
run_step_asset_counts AS (
    SELECT run_id, step_key, COUNT(*) AS asset_count
    FROM run_asset_steps
    GROUP BY run_id, step_key
),
run_asset_node_output_costs AS (
    SELECT
        run_asset_steps.run_id,
        split_part(run_asset_steps.step_key, '.', 1)
            AS asset_node,
        run_asset_steps.asset_key,
        SUM(
            GREATEST(
                EXTRACT(
                    EPOCH FROM (
                        run_step_ends.ended_at
                        - run_step_starts.started_at
                    )
                ),
                0.0
            ) / NULLIF(run_step_asset_counts.asset_count, 0)
        ) AS asset_output_step_seconds
    FROM run_asset_steps
    INNER JOIN run_step_starts
        ON run_step_starts.run_id = run_asset_steps.run_id
        AND run_step_starts.step_key = run_asset_steps.step_key
    INNER JOIN run_step_ends
        ON run_step_ends.run_id = run_asset_steps.run_id
        AND run_step_ends.step_key = run_asset_steps.step_key
    INNER JOIN run_step_asset_counts
        ON run_step_asset_counts.run_id = run_asset_steps.run_id
        AND run_step_asset_counts.step_key = run_asset_steps.step_key
    WHERE run_step_ends.ended_at > run_step_starts.started_at
    GROUP BY
        run_asset_steps.run_id,
        split_part(run_asset_steps.step_key, '.', 1),
        run_asset_steps.asset_key
),
run_asset_node_weight_bases AS (
    SELECT
        run_asset_nodes.run_id,
        run_asset_nodes.asset_node,
        run_asset_nodes.asset_key,
        COALESCE(
            run_asset_node_output_costs.asset_output_step_seconds,
            0.0
        ) AS asset_output_step_seconds,
        SUM(
            COALESCE(
                run_asset_node_output_costs.asset_output_step_seconds,
                0.0
            )
        ) OVER (
            PARTITION BY
                run_asset_nodes.run_id,
                run_asset_nodes.asset_node
        ) AS node_output_step_seconds,
        COUNT(*) OVER (
            PARTITION BY
                run_asset_nodes.run_id,
                run_asset_nodes.asset_node
        ) AS node_asset_count
    FROM run_asset_nodes
    LEFT JOIN run_asset_node_output_costs
        ON run_asset_node_output_costs.run_id = run_asset_nodes.run_id
        AND run_asset_node_output_costs.asset_node
            = run_asset_nodes.asset_node
        AND run_asset_node_output_costs.asset_key
            = run_asset_nodes.asset_key
),
run_asset_node_weights AS (
    SELECT
        run_id,
        asset_node,
        asset_key,
        CASE
            WHEN node_output_step_seconds > 0.0
                THEN asset_output_step_seconds
                    / node_output_step_seconds
            ELSE 1.0 / node_asset_count
        END AS asset_node_weight
    FROM run_asset_node_weight_bases
),
run_asset_costs AS (
    SELECT
        run_asset_node_weights.run_id,
        run_asset_node_weights.asset_key,
        SUM(
            run_node_costs.node_step_seconds
            * run_asset_node_weights.asset_node_weight
        ) AS asset_step_seconds
    FROM run_asset_node_weights
    INNER JOIN run_node_costs
        ON run_node_costs.run_id = run_asset_node_weights.run_id
        AND run_node_costs.asset_node
            = run_asset_node_weights.asset_node
    GROUP BY
        run_asset_node_weights.run_id,
        run_asset_node_weights.asset_key
),
run_asset_weight_bases AS (
    SELECT
        run_assets.run_id,
        run_assets.asset_key,
        COALESCE(run_asset_costs.asset_step_seconds, 0.0)
            AS asset_step_seconds,
        SUM(COALESCE(run_asset_costs.asset_step_seconds, 0.0)) OVER (
            PARTITION BY run_assets.run_id
        ) AS run_step_seconds,
        COUNT(*) OVER (
            PARTITION BY run_assets.run_id
        ) AS run_asset_count
    FROM run_assets
    LEFT JOIN run_asset_costs
        ON run_asset_costs.run_id = run_assets.run_id
        AND run_asset_costs.asset_key = run_assets.asset_key
),
run_asset_weights AS (
    SELECT
        run_id,
        asset_key,
        CASE
            WHEN run_step_seconds > 0.0
                THEN asset_step_seconds / run_step_seconds
            ELSE 1.0 / run_asset_count
        END AS asset_weight
    FROM run_asset_weight_bases
),
bounded_run_intervals AS (
    SELECT
        runs.run_id,
        to_timestamp(runs.start_time) AT TIME ZONE 'UTC'
            AS started_at,
        CASE
            WHEN runs.end_time IS NOT NULL
                THEN LEAST(
                    to_timestamp(runs.end_time) AT TIME ZONE 'UTC',
                    CAST(:to_utc AS TIMESTAMP)
                )
            WHEN runs.status IN ('STARTING', 'STARTED', 'CANCELING')
                THEN CAST(:to_utc AS TIMESTAMP)
        END AS ended_at
    FROM runs
    INNER JOIN (
        SELECT DISTINCT run_id
        FROM run_assets
    ) AS asset_runs
        ON asset_runs.run_id = runs.run_id
    WHERE
        runs.start_time IS NOT NULL
        AND to_timestamp(runs.start_time) AT TIME ZONE 'UTC'
            < CAST(:to_utc AS TIMESTAMP)
),
run_intervals AS (
    SELECT run_id, started_at, ended_at
    FROM bounded_run_intervals
    WHERE
        ended_at > started_at
        AND (
            :from_utc IS NULL
            OR ended_at > CAST(:from_utc AS TIMESTAMP)
        )
),
run_daily_intervals AS (
    SELECT
        run_intervals.run_id,
        CAST(days.day_start AS DATE) AS metric_date,
        GREATEST(
            run_intervals.started_at,
            days.day_start
        ) AS interval_start,
        LEAST(
            run_intervals.ended_at,
            days.day_start + INTERVAL '1 day'
        ) AS interval_end
    FROM run_intervals
    CROSS JOIN LATERAL generate_series(
        date_trunc(
            'day',
            CASE
                WHEN :from_utc IS NULL
                    THEN run_intervals.started_at
                ELSE GREATEST(
                    run_intervals.started_at,
                    CAST(:from_utc AS TIMESTAMP)
                )
            END
        ),
        date_trunc('day', run_intervals.ended_at),
        INTERVAL '1 day'
    ) AS days(day_start)
    WHERE
        LEAST(
            run_intervals.ended_at,
            days.day_start + INTERVAL '1 day'
        ) > GREATEST(
            run_intervals.started_at,
            days.day_start
        )
),
run_interval_boundaries AS (
    SELECT metric_date, interval_start AS boundary
    FROM run_daily_intervals
    UNION
    SELECT metric_date, interval_end AS boundary
    FROM run_daily_intervals
),
run_timeline_segments AS (
    SELECT
        metric_date,
        boundary AS segment_start,
        LEAD(boundary) OVER (
            PARTITION BY metric_date
            ORDER BY boundary
        ) AS segment_end
    FROM run_interval_boundaries
),
active_run_segments AS (
    SELECT
        run_timeline_segments.metric_date,
        run_daily_intervals.run_id,
        EXTRACT(
            EPOCH FROM (
                run_timeline_segments.segment_end
                - run_timeline_segments.segment_start
            )
        ) / COUNT(*) OVER (
            PARTITION BY
                run_timeline_segments.metric_date,
                run_timeline_segments.segment_start,
                run_timeline_segments.segment_end
        ) AS attributed_run_seconds
    FROM run_timeline_segments
    INNER JOIN run_daily_intervals
        ON run_daily_intervals.metric_date
            = run_timeline_segments.metric_date
        AND run_daily_intervals.interval_start
            < run_timeline_segments.segment_end
        AND run_daily_intervals.interval_end
            > run_timeline_segments.segment_start
    WHERE
        run_timeline_segments.segment_end
            > run_timeline_segments.segment_start
),
run_busy_aggregates AS (
    SELECT
        metric_date,
        run_id,
        SUM(attributed_run_seconds) AS attributed_run_seconds
    FROM active_run_segments
    GROUP BY metric_date, run_id
),
asset_busy_aggregates AS (
    SELECT
        run_busy_aggregates.metric_date,
        run_asset_weights.asset_key,
        SUM(
            run_busy_aggregates.attributed_run_seconds
            * run_asset_weights.asset_weight
        ) / 60.0 AS minutes_on_job
    FROM run_busy_aggregates
    INNER JOIN run_asset_weights
        ON run_asset_weights.run_id = run_busy_aggregates.run_id
    GROUP BY
        run_busy_aggregates.metric_date,
        run_asset_weights.asset_key
),
execution_aggregates AS (
    SELECT
        asset_key,
        CAST(terminal_at AS DATE) AS metric_date,
        CAST(COUNT(*) AS INTEGER) AS execution_count,
        CAST(
            COUNT(*) FILTER (WHERE materialized_at IS NOT NULL)
            AS INTEGER
        ) AS materialization_count,
        CAST(
            COUNT(*) FILTER (
                WHERE materialized_at IS NULL AND failed_at IS NOT NULL
            )
            AS INTEGER
        ) AS execution_failure_count,
        AVG(
            GREATEST(
                EXTRACT(EPOCH FROM (terminal_at - started_at)),
                0.0
            )
        ) FILTER (
            WHERE materialized_at IS NOT NULL
        ) AS average_time_to_complete_seconds,
        AVG(queued_seconds) AS average_queued_time_seconds,
        jsonb_agg(
            jsonb_build_object(
                'run_id', run_id,
                'step_key', step_key,
                'partition', partition,
                'status', CASE
                    WHEN materialized_at IS NOT NULL THEN 'MATERIALIZED'
                    ELSE 'FAILED'
                END,
                'planned_at_utc', to_char(
                    planned_at,
                    'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
                ),
                'started_at_utc', to_char(
                    started_at,
                    'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
                ),
                'terminal_at_utc', to_char(
                    terminal_at,
                    'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
                ),
                'duration_seconds', CAST(
                    GREATEST(
                        EXTRACT(EPOCH FROM (terminal_at - started_at)),
                        0.0
                    ) AS NUMERIC(18, 3)
                ),
                'queued_seconds', CAST(
                    queued_seconds AS NUMERIC(18, 3)
                )
            )
            ORDER BY terminal_at, run_id, step_key, partition
        )::text AS step_details_json
    FROM terminal_attempts
    WHERE
        started_at IS NOT NULL
        AND terminal_at IS NOT NULL
        AND terminal_at < CAST(:to_utc AS TIMESTAMP)
        AND (
            :from_utc IS NULL
            OR terminal_at >= CAST(:from_utc AS TIMESTAMP)
        )
    GROUP BY asset_key, CAST(terminal_at AS DATE)
),
terminal_asset_checks AS (
    SELECT
        asset_check_executions.asset_key,
        asset_check_executions.run_id,
        asset_check_executions.check_name,
        asset_check_executions.partition,
        asset_check_executions.evaluation_event_storage_id,
        asset_check_executions.materialization_event_storage_id,
        CASE
            WHEN asset_check_executions.execution_status IN (
                'SUCCEEDED',
                'FAILED'
            )
                THEN asset_check_executions.evaluation_event_timestamp
            WHEN
                asset_check_executions.execution_status = 'PLANNED'
                AND runs.status = 'FAILURE'
                THEN to_timestamp(runs.end_time) AT TIME ZONE 'UTC'
        END AS terminal_at,
        CASE
            WHEN asset_check_executions.execution_status = 'PLANNED'
                THEN 'EXECUTION_FAILED'
            ELSE asset_check_executions.execution_status
        END AS resolved_status,
        CASE
            WHEN asset_check_executions.execution_status = 'FAILED' THEN 1
            WHEN
                asset_check_executions.execution_status = 'PLANNED'
                AND runs.status = 'FAILURE'
                THEN 1
            ELSE 0
        END AS check_failed
    FROM asset_check_executions
    INNER JOIN runs
        ON runs.run_id = asset_check_executions.run_id
    WHERE
        asset_check_executions.asset_key IS NOT NULL
        AND asset_check_executions.asset_key::jsonb ->> 0 = :user_code
        AND (
            asset_check_executions.execution_status IN (
                'SUCCEEDED',
                'FAILED'
            )
            OR (
                asset_check_executions.execution_status = 'PLANNED'
                AND runs.status = 'FAILURE'
            )
        )
),
asset_check_aggregates AS (
    SELECT
        asset_key,
        CAST(terminal_at AS DATE) AS metric_date,
        CAST(COUNT(*) AS INTEGER) AS asset_check_count,
        CAST(SUM(check_failed) AS INTEGER) AS asset_check_failure_count,
        jsonb_agg(
            jsonb_build_object(
                'run_id', run_id,
                'check_name', check_name,
                'partition', partition,
                'status', resolved_status,
                'evaluated_at_utc', to_char(
                    terminal_at,
                    'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
                ),
                'evaluation_event_storage_id',
                    evaluation_event_storage_id,
                'materialization_event_storage_id',
                    materialization_event_storage_id
            )
            ORDER BY terminal_at, run_id, check_name
        )::text AS asset_check_details_json
    FROM terminal_asset_checks
    WHERE
        terminal_at IS NOT NULL
        AND terminal_at < CAST(:to_utc AS TIMESTAMP)
        AND (
            :from_utc IS NULL
            OR terminal_at >= CAST(:from_utc AS TIMESTAMP)
        )
    GROUP BY asset_key, CAST(terminal_at AS DATE)
),
asset_dates AS (
    SELECT asset_key, metric_date
    FROM execution_aggregates
    UNION
    SELECT asset_key, metric_date
    FROM asset_check_aggregates
    UNION
    SELECT asset_key, metric_date
    FROM asset_busy_aggregates
    UNION
    SELECT asset_key, metric_date
    FROM asset_row_aggregates
)
SELECT
    :user_code AS user_code,
    asset_dates.asset_key,
    asset_dates.metric_date,
    COALESCE(execution_aggregates.execution_count, 0) AS execution_count,
    COALESCE(
        execution_aggregates.materialization_count,
        0
    ) AS materialization_count,
    asset_row_aggregates.inserted_row_count,
    asset_row_aggregates.merged_row_count,
    COALESCE(
        execution_aggregates.execution_failure_count,
        0
    ) AS execution_failure_count,
    COALESCE(asset_check_aggregates.asset_check_count, 0) AS asset_check_count,
    COALESCE(
        asset_check_aggregates.asset_check_failure_count,
        0
    ) AS asset_check_failure_count,
    CAST(
        execution_aggregates.average_time_to_complete_seconds
        AS NUMERIC(18, 3)
    ) AS average_time_to_complete_seconds,
    (
        COALESCE(execution_aggregates.execution_failure_count, 0)
        + COALESCE(asset_check_aggregates.asset_check_failure_count, 0)
    ) > 0 AS has_failed,
    (
        COALESCE(execution_aggregates.execution_failure_count, 0)
        + COALESCE(asset_check_aggregates.asset_check_failure_count, 0)
    ) AS number_of_failures,
    CAST(
        execution_aggregates.average_queued_time_seconds
        AS NUMERIC(18, 3)
    ) AS average_queued_time_seconds,
    CAST(
        COALESCE(asset_busy_aggregates.minutes_on_job, 0.0)
        AS NUMERIC(18, 3)
    ) AS minutes_on_job,
    CAST(
        COALESCE(asset_busy_aggregates.minutes_on_job, 0.0)
        / CASE
            WHEN asset_dates.metric_date = CAST(:to_utc AS DATE)
                THEN GREATEST(
                    EXTRACT(
                        EPOCH FROM (
                            CAST(:to_utc AS TIMESTAMP)
                            - date_trunc(
                                'day',
                                CAST(:to_utc AS TIMESTAMP)
                            )
                        )
                    ) / 60.0,
                    1.0
                )
            ELSE 1440.0
        END
        * 100.0
        AS NUMERIC(9, 4)
    ) AS time_in_job_percentage,
    COALESCE(
        execution_aggregates.step_details_json,
        '[]'
    ) AS step_details_json,
    COALESCE(
        asset_check_aggregates.asset_check_details_json,
        '[]'
    ) AS asset_check_details_json
FROM asset_dates
LEFT JOIN execution_aggregates
    ON execution_aggregates.asset_key = asset_dates.asset_key
    AND execution_aggregates.metric_date = asset_dates.metric_date
LEFT JOIN asset_check_aggregates
    ON asset_check_aggregates.asset_key = asset_dates.asset_key
    AND asset_check_aggregates.metric_date = asset_dates.metric_date
LEFT JOIN asset_busy_aggregates
    ON asset_busy_aggregates.asset_key = asset_dates.asset_key
    AND asset_busy_aggregates.metric_date = asset_dates.metric_date
LEFT JOIN asset_row_aggregates
    ON asset_row_aggregates.asset_key = asset_dates.asset_key
    AND asset_row_aggregates.metric_date = asset_dates.metric_date
ORDER BY asset_dates.metric_date, asset_dates.asset_key
"""
FACT_DAGSTER_ASSET_EXECUTION_DAILY_MERGE_SQL = """
MERGE observability.factDagsterAssetExecutionDaily WITH (HOLDLOCK) AS target
USING (
    SELECT
        CAST(? AS VARCHAR(64)) AS UserCode,
        CAST(? AS VARCHAR(500)) AS AssetKey,
        CAST(? AS DATE) AS [Date],
        CAST(? AS INT) AS ExecutionCount,
        CAST(? AS INT) AS MaterializationCount,
        CAST(? AS BIGINT) AS InsertedRowCount,
        CAST(? AS BIGINT) AS MergedRowCount,
        CAST(? AS INT) AS ExecutionFailureCount,
        CAST(? AS INT) AS AssetCheckCount,
        CAST(? AS INT) AS AssetCheckFailureCount,
        CAST(? AS DECIMAL(18, 3)) AS AverageTimeToCompleteSeconds,
        CAST(? AS BIT) AS HasFailed,
        CAST(? AS INT) AS NumberOfFailures,
        CAST(? AS DECIMAL(18, 3)) AS AverageQueuedTimeSeconds,
        CAST(? AS DECIMAL(18, 3)) AS MinutesOnJob,
        CAST(? AS DECIMAL(9, 4)) AS TimeInJobPercentage,
        CAST(? AS NVARCHAR(MAX)) AS StepDetailsJson,
        CAST(? AS NVARCHAR(MAX)) AS AssetCheckDetailsJson
) AS source
ON
    target.[Date] = source.[Date]
    AND target.UserCode = source.UserCode
    AND target.AssetKey = source.AssetKey
WHEN MATCHED THEN
    UPDATE SET
        ExecutionCount = source.ExecutionCount,
        MaterializationCount = source.MaterializationCount,
        InsertedRowCount = source.InsertedRowCount,
        MergedRowCount = source.MergedRowCount,
        ExecutionFailureCount = source.ExecutionFailureCount,
        AssetCheckCount = source.AssetCheckCount,
        AssetCheckFailureCount = source.AssetCheckFailureCount,
        AverageTimeToCompleteSeconds = source.AverageTimeToCompleteSeconds,
        HasFailed = source.HasFailed,
        NumberOfFailures = source.NumberOfFailures,
        AverageQueuedTimeSeconds = source.AverageQueuedTimeSeconds,
        MinutesOnJob = source.MinutesOnJob,
        TimeInJobPercentage = source.TimeInJobPercentage,
        StepDetailsJson = source.StepDetailsJson,
        AssetCheckDetailsJson = source.AssetCheckDetailsJson,
        ModifiedAt = SYSUTCDATETIME()
WHEN NOT MATCHED THEN
    INSERT (
        UserCode,
        AssetKey,
        [Date],
        ExecutionCount,
        MaterializationCount,
        InsertedRowCount,
        MergedRowCount,
        ExecutionFailureCount,
        AssetCheckCount,
        AssetCheckFailureCount,
        AverageTimeToCompleteSeconds,
        HasFailed,
        NumberOfFailures,
        AverageQueuedTimeSeconds,
        MinutesOnJob,
        TimeInJobPercentage,
        StepDetailsJson,
        AssetCheckDetailsJson
    )
    VALUES (
        source.UserCode,
        source.AssetKey,
        source.[Date],
        source.ExecutionCount,
        source.MaterializationCount,
        source.InsertedRowCount,
        source.MergedRowCount,
        source.ExecutionFailureCount,
        source.AssetCheckCount,
        source.AssetCheckFailureCount,
        source.AverageTimeToCompleteSeconds,
        source.HasFailed,
        source.NumberOfFailures,
        source.AverageQueuedTimeSeconds,
        source.MinutesOnJob,
        source.TimeInJobPercentage,
        source.StepDetailsJson,
        source.AssetCheckDetailsJson
    );
"""


@asset(
    required_resource_keys={"sqlserver"},
    group_name="mcdonalds_cdp",
    key_prefix=["mcdonalds_cdp"],
    retry_policy=FACT_DAGSTER_ASSET_EXECUTION_DAILY_RETRY_POLICY,
)
def observability_factdagsterassetexecutiondaily(
    context: AssetExecutionContext,
) -> MaterializeResult:
    start_time = perf_counter()
    sqlserver: SqlServerResource = context.resources.sqlserver
    last_loaded_at = sqlserver.get_watermark(
        FACT_DAGSTER_ASSET_EXECUTION_DAILY_INGESTION_NAME
    )
    from_utc = None
    if last_loaded_at is not None:
        from_utc = datetime(
            last_loaded_at.year,
            last_loaded_at.month,
            last_loaded_at.day,
        ) - timedelta(days=1)
    to_utc = datetime.now(timezone.utc).replace(tzinfo=None)
    sqlserver.mark_run_started(
        FACT_DAGSTER_ASSET_EXECUTION_DAILY_INGESTION_NAME
    )

    try:
        with context.instance.run_storage.connect() as connection:
            rows = connection.execute(
                text(DAGSTER_ASSET_EXECUTION_DAILY_SQL),
                {
                    "user_code": "mcdonalds_cdp",
                    "from_utc": from_utc,
                    "to_utc": to_utc,
                },
            ).mappings().all()

        for row in rows:
            asset_key = "/".join(json.loads(row["asset_key"]))
            merge_result = sqlserver.execute_query(
                FACT_DAGSTER_ASSET_EXECUTION_DAILY_MERGE_SQL,
                row["user_code"],
                asset_key,
                row["metric_date"],
                row["execution_count"],
                row["materialization_count"],
                row["inserted_row_count"],
                row["merged_row_count"],
                row["execution_failure_count"],
                row["asset_check_count"],
                row["asset_check_failure_count"],
                row["average_time_to_complete_seconds"],
                row["has_failed"],
                row["number_of_failures"],
                row["average_queued_time_seconds"],
                row["minutes_on_job"],
                row["time_in_job_percentage"],
                row["step_details_json"],
                row["asset_check_details_json"],
            )
            if isinstance(merge_result, Exception):
                raise merge_result
            if merge_result is None:
                raise RuntimeError(
                    "SQL Server did not confirm the Dagster asset execution merge"
                )
    except Exception as ex:
        sqlserver.mark_run_completed(
            ingestion_name=(
                FACT_DAGSTER_ASSET_EXECUTION_DAILY_INGESTION_NAME
            ),
            new_last_loaded_at=last_loaded_at,
            succeeded=False,
            error_message=str(ex),
        )
        raise

    sqlserver.mark_run_completed(
        ingestion_name=FACT_DAGSTER_ASSET_EXECUTION_DAILY_INGESTION_NAME,
        new_last_loaded_at=to_utc,
        succeeded=True,
        error_message=None,
    )

    return MaterializeResult(
        metadata={
            "row_count": len(rows),
            "elapsed_seconds": round(perf_counter() - start_time, 3),
            "from_utc": str(from_utc) if from_utc is not None else "all_available",
            "to_utc": str(to_utc),
        },
    )
