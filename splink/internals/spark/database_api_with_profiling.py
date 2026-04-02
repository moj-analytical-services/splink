from __future__ import annotations

import re
from os import PathLike
from pathlib import Path
from time import perf_counter_ns
from typing import Any

from pyspark.sql.dataframe import DataFrame as spark_df

from .database_api import SparkAPI
from .dataframe import SparkDataFrame


class SparkAPIWithProfiling(SparkAPI):
    def __init__(
        self,
        *,
        spark_session: Any,
        break_lineage_method: str | None = None,
        catalog: str | None = None,
        database: str | None = None,
        repartition_after_blocking: bool = False,
        num_partitions_on_repartition: int | None = None,
        register_udfs_automatically: bool = True,
        query_profiling_dir: str | PathLike[str] = "tmp_query_profiling",
    ):
        super().__init__(
            spark_session=spark_session,
            break_lineage_method=break_lineage_method,
            catalog=catalog,
            database=database,
            repartition_after_blocking=repartition_after_blocking,
            num_partitions_on_repartition=num_partitions_on_repartition,
            register_udfs_automatically=register_udfs_automatically,
        )
        self.query_profiling_dir = Path(query_profiling_dir)
        self.query_profiling_dir.mkdir(parents=True, exist_ok=True)
        self._query_profile_counter = 0
        self._pending_profile_path: Path | None = None
        self._pending_profile_sql: str | None = None

    def _should_profile_sql(self, sql: str) -> bool:
        stripped_sql = sql.lstrip().upper()
        return stripped_sql.startswith("SELECT") or stripped_sql.startswith("WITH")

    def _next_query_profile_path(self, templated_name: str) -> Path:
        self._query_profile_counter += 1
        safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", templated_name).strip("_")
        if not safe_name:
            safe_name = "query"
        filename = f"{self._query_profile_counter:04d}_{safe_name}_spark.txt"
        return self.query_profiling_dir / filename

    @staticmethod
    def _iter_scala_seq(seq):
        for index in range(seq.size()):
            yield seq.apply(index)

    @staticmethod
    def _format_spark_metric_value(metric: Any) -> str:
        value = metric.value()
        metric_type = metric.metricType()

        if metric_type == "timing":
            return f"{value} ms"
        if metric_type == "nsTiming":
            return f"{value} ns ({value / 1_000_000:.3f} ms)"
        if metric_type == "size":
            return f"{value} bytes"
        if metric_type == "average":
            return f"{value / 10:.1f}"
        if metric_type == "sum":
            return str(value)
        return f"{value} ({metric_type})"

    @staticmethod
    def _spark_metric_name(metric: Any, fallback_name: str) -> str:
        try:
            metric_name = metric.name()
            if metric_name.isDefined():
                return metric_name.get()
        except Exception:
            pass

        return fallback_name

    def _spark_plan_metrics_as_text(self, node: Any, depth: int = 0) -> str:
        indent = "  " * depth
        lines = [f"{indent}{node.nodeName()}"]

        metrics = node.metrics()
        keys = metrics.keys().toSeq()
        for index in range(keys.size()):
            key = keys.apply(index)
            metric = metrics.apply(key)
            metric_name = self._spark_metric_name(metric, str(key))
            metric_value = self._format_spark_metric_value(metric)
            lines.append(f"{indent}  {metric_name} = {metric_value}")

        children = list(self._iter_scala_seq(node.children()))
        if not children:
            try:
                stage_plan = node.plan()
            except Exception:
                stage_plan = None
            if stage_plan is not None:
                children = [stage_plan]

        for child in children:
            lines.append(self._spark_plan_metrics_as_text(child, depth + 1))

        return "\n".join(lines)

    def _spark_profile_text(self, sql: str, df: spark_df, duration_ns: int) -> str:
        executed_plan = df._jdf.queryExecution().executedPlan()
        try:
            final_plan = executed_plan.finalPhysicalPlan()
        except Exception:
            final_plan = executed_plan

        return "\n\n".join(
            [
                "== SQL ==",
                sql.strip(),
                "== Final Physical Plan ==",
                final_plan.treeString().strip(),
                "== Total Runtime ==",
                f"{duration_ns} ns ({duration_ns / 1_000_000:.3f} ms)",
                "== Runtime Metrics ==",
                self._spark_plan_metrics_as_text(final_plan),
            ]
        )

    def _setup_for_execute_sql(self, sql: str, physical_name: str) -> str:
        final_sql = super()._setup_for_execute_sql(sql, physical_name)

        if self._should_profile_sql(sql):
            self._pending_profile_path = self._next_query_profile_path(physical_name)
            self._pending_profile_sql = final_sql
        else:
            self._pending_profile_path = None
            self._pending_profile_sql = None

        return final_sql

    def _cleanup_for_execute_sql(
        self, table: spark_df, templated_name: str, physical_name: str
    ) -> SparkDataFrame:
        profile_path = self._pending_profile_path
        profile_sql = self._pending_profile_sql
        if profile_path is None or profile_sql is None:
            return super()._cleanup_for_execute_sql(
                table, templated_name, physical_name
            )

        try:
            start_time_ns = perf_counter_ns()
            spark_df = self._break_lineage_and_repartition(
                table, templated_name, physical_name
            )
            spark_df.count()

            duration_ns = perf_counter_ns() - start_time_ns
            profile_path.write_text(
                self._spark_profile_text(profile_sql, spark_df, duration_ns),
                encoding="utf-8",
            )

            spark_df.createOrReplaceTempView(physical_name)
            return self.table_to_splink_dataframe(templated_name, physical_name)
        finally:
            self._pending_profile_path = None
            self._pending_profile_sql = None
