from dagster import Definitions

from .resources import spark_resource, sqlserver_resource

defs = Definitions(
    resources={
        "spark": spark_resource,
        "sqlserver": sqlserver_resource,
    },
)

__all__ = ["defs"]
