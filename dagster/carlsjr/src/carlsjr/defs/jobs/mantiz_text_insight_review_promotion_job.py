from dagster import Out, RetryPolicy, job, op

from ..resources import SqlServerResource


PROMOTION_RETRY_POLICY = RetryPolicy(max_retries=1, delay=60)


@op(
    required_resource_keys={"sqlserver"},
    out=Out(dict),
    retry_policy=PROMOTION_RETRY_POLICY,
)
def promote_mantiz_text_insight_reviews_op(context) -> dict:
    sql: SqlServerResource = context.resources.sqlserver
    promoted_rows = sql.promote_mantiz_text_insight_reviews()
    metadata = {"promoted_rows": promoted_rows}
    context.add_output_metadata(metadata)
    return metadata


@job(name="102_mantiz_text_insight_review_promotion", tags={"job_type": "mantiz", "run_lane": "operational", "user_code_run_lane": "carlsjr_operational", "user_code": "carlsjr"})
def mantiz_text_insight_review_promotion_job():
    promote_mantiz_text_insight_reviews_op()
