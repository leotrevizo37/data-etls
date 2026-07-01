from dagster import Out, RetryPolicy, job, op

from ..resources import SqlServerResource


PROMOTION_RETRY_POLICY = RetryPolicy(max_retries=1, delay=60)


@op(
    required_resource_keys={"sqlserver"},
    out=Out(dict),
    retry_policy=PROMOTION_RETRY_POLICY,
)
def promote_smartaudits_comment_lookup_op(context) -> dict:
    sql: SqlServerResource = context.resources.sqlserver
    promoted_rows = sql.promote_smartaudits_reviewed_comments()
    metadata = {"promoted_rows": promoted_rows}
    context.add_output_metadata(metadata)
    return metadata


@job(name="112_smartaudits_comment_lookup_promotion", tags={"job_type": "smartaudits", "user_code": "carlsjr"})
def smartaudits_comment_lookup_promotion_job():
    promote_smartaudits_comment_lookup_op()
