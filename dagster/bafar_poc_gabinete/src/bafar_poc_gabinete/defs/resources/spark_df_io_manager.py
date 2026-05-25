from __future__ import annotations

from dagster import IOManager, InputContext, OutputContext, io_manager


class InMemorySparkDFIOManager(IOManager):
    @staticmethod
    def _key(context) -> str:
        return "/".join(str(p) for p in context.get_identifier())

    def handle_output(self, context: OutputContext, obj) -> None:
        if obj is None:
            return
        key = self._key(context)
        spark_res = context.resources.spark
        spark_res.store_df(key, obj)
        context.add_output_metadata({"io_strategy": "in_memory_spark_persist", "key": key})

    def load_input(self, context: InputContext):
        upstream = context.upstream_output
        if upstream is None:
            raise RuntimeError("Missing upstream output for Spark DataFrame input.")
        key = self._key(upstream)
        spark_res = context.resources.spark
        df = spark_res.load_df(key)
        if df is None:
            raise RuntimeError(
                f"DataFrame '{key}' no está en el store del SparkResource. "
                "El op upstream debe re-ejecutarse (posiblemente la sesión Spark fue reiniciada)."
            )
        return df


@io_manager(required_resource_keys={"spark"})
def spark_df_io_manager(_):
    return InMemorySparkDFIOManager()
