from __future__ import annotations

import logging
import re
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, List, Optional

import sqlglot
from sqlglot.errors import ParseError
from sqlglot.expressions import Table

from splink.internals.misc import ensure_is_list, indent_sql, normalise_sql

from .splink_dataframe import SplinkDataFrame

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from splink.internals.database_api import DatabaseAPISubClass


class CTE:
    def __init__(
        self, sql: str, output_table_name: str, materialized: bool = False
    ) -> None:
        self.sql = sql
        self.output_table_name = output_table_name
        self.materialized = materialized

    @property
    def _uses_tables(self):
        try:
            tree = sqlglot.parse_one(self.sql, read=None)
        except ParseError:
            return ["Failure to parse SQL - tablenames not known"]

        return list({t.sql() for t in tree.find_all(Table)})

    @property
    def cte_description(self):
        uses_tables = ", ".join(self._uses_tables)
        uses_tables = f" {uses_tables} "

        return (
            f"CTE reads tables [{uses_tables}]"
            f" and has output table name: {self.output_table_name}"
        )

    def __repr__(self) -> str:
        return self.cte_description


class CTEPipeline:
    def __init__(self, input_dataframes: Optional[List[SplinkDataFrame]] = None):
        self.queue: List[CTE] = []

        if input_dataframes is None:
            self.input_dataframes: list[SplinkDataFrame] = []
        else:
            self.input_dataframes = ensure_is_list(input_dataframes)

        # A flag to ensure that a pipeline cannot be reused
        self.spent = False

    def _enqueue_sql(
        self, sql: str, output_table_name: str, materialized: bool
    ) -> None:
        if self.spent:
            raise ValueError("This pipeline has already been used")
        sql_task = CTE(sql, output_table_name, materialized)
        self.queue.append(sql_task)

    def enqueue_sql(self, sql: str, output_table_name: str) -> None:
        self._enqueue_sql(sql, output_table_name, False)

    def enqueue_sql_materialized(self, sql: str, output_table_name: str) -> None:
        self._enqueue_sql(sql, output_table_name, True)

    def enqueue_list_of_sqls(self, sql_list: Sequence[Mapping[str, object]]) -> None:
        for sql_dict in sql_list:
            sql = sql_dict["sql"]
            output_table_name = sql_dict["output_table_name"]
            materialized = sql_dict.get("materialized", False)
            if not isinstance(sql, str) or not isinstance(output_table_name, str):
                raise TypeError("Pipeline SQL and output table name must be strings")
            if not isinstance(materialized, bool):
                raise TypeError("Pipeline materialized flag must be a boolean")
            self._enqueue_sql(sql, output_table_name, materialized)

    def break_lineage(self, db_api: "DatabaseAPISubClass") -> "CTEPipeline":
        df = db_api.sql_pipeline_to_splink_dataframe(self)
        new_pipeline = CTEPipeline(input_dataframes=[df])
        return new_pipeline

    def append_input_dataframe(self, df: SplinkDataFrame) -> None:
        self.input_dataframes.append(df)

    @staticmethod
    def _replace_templated_identifier_with_physical_name(
        sql: str, templated_name: str, physical_name: str
    ) -> str:
        # Replace only whole SQL identifiers, preserving matching quotes.
        # This matches cases like:
        #   from __splink__df_concat_with_tf)
        #   from __splink__df_concat_with_tf,
        #   from "__splink__df_concat_with_tf" as l
        # but not longer identifiers like:
        #   __splink__df_concat_with_tf_left
        pattern = (
            rf'(?<!\w)(?P<quote>["`]?){re.escape(templated_name)}' rf"(?P=quote)(?!\w)"
        )

        def _replacement(match: re.Match[str]) -> str:
            quote = match.group("quote")
            return f"{quote}{physical_name}{quote}"

        return re.sub(pattern, _replacement, sql)

    def _replace_templated_references_with_physical_names(self, sql: str) -> str:
        replacements = sorted(
            (
                (df.templated_name, df.physical_name)
                for df in self.input_dataframes
                if not df.physical_and_template_names_equal
            ),
            key=lambda pair: len(pair[0]),
            reverse=True,
        )
        for templated_name, physical_name in replacements:
            sql = self._replace_templated_identifier_with_physical_name(
                sql, templated_name, physical_name
            )
        return sql

    def _resolved_queue(self):
        return [
            CTE(
                self._replace_templated_references_with_physical_names(cte.sql),
                cte.output_table_name,
                cte.materialized,
            )
            for cte in self.queue
        ]

    def _log_pipeline(self, parts):
        if logger.isEnabledFor(7):
            inputs = ", ".join(df.physical_name for df in self.input_dataframes)
            logger.log(
                7,
                f"SQL pipeline was passed inputs [{inputs}] and output "
                f"dataset {parts[-1].output_table_name}",
            )

            for i, part in enumerate(parts):
                logger.log(7, f"    Pipeline part {i + 1}: {part.cte_description}")

    def ctes_pipeline(self) -> List[CTE]:
        """Common table expressions"""
        return self._resolved_queue()

    def generate_cte_pipeline_sql(self) -> str:
        self.spent = True

        pipeline = self.ctes_pipeline()

        self._log_pipeline(pipeline)

        with_ctes_pipeline = pipeline[:-1]
        final_query = pipeline[-1]

        with_ctes = []
        for part in with_ctes_pipeline:
            hint = " MATERIALIZED" if part.materialized else ""
            with_ctes.append(
                f"{part.output_table_name} as{hint} (\n{indent_sql(part.sql)}\n)"
            )
        with_ctes_str = ", \n\n".join(with_ctes)
        if with_ctes_str:
            with_ctes_str = f"WITH\n\n{with_ctes_str}\n"

        final_sql = with_ctes_str + normalise_sql(final_query.sql)

        return final_sql

    @property
    def output_table_name(self):
        return self.ctes_pipeline()[-1].output_table_name
