from __future__ import annotations

import logging
from typing import TYPE_CHECKING, List, Optional

import sqlglot
from sqlglot.errors import ParseError
from sqlglot.expressions import Table

from splink.internals.misc import ensure_is_list

from .splink_dataframe import SplinkDataFrame

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from splink.internals.database_api import DatabaseAPISubClass


class CTE:
    def __init__(self, sql, output_table_name):
        self.sql = sql
        self.output_table_name = output_table_name

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

    def enqueue_sql(self, sql: str, output_table_name: str) -> None:
        if self.spent:
            raise ValueError("This pipeline has already been used")
        sql_task = CTE(sql, output_table_name)
        self.queue.append(sql_task)

    def enqueue_list_of_sqls(self, sql_list: List[dict[str, str]]) -> None:
        for sql_dict in sql_list:
            self.enqueue_sql(sql_dict["sql"], sql_dict["output_table_name"])

    def break_lineage(self, db_api: "DatabaseAPISubClass") -> "CTEPipeline":
        df = db_api.sql_pipeline_to_splink_dataframe(self)
        new_pipeline = CTEPipeline(input_dataframes=[df])
        return new_pipeline

    def append_input_dataframe(self, df: SplinkDataFrame) -> None:
        self.input_dataframes.append(df)

    def _input_dataframes_as_cte(self):
        return [
            CTE(f"\nselect * from {df.physical_name}", df.templated_name)
            for df in self.input_dataframes
            if not df.physical_and_template_names_equal
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
                logger.log(7, f"    Pipeline part {i+1}: {part.cte_description}")

    def ctes_pipeline(self) -> List[CTE]:
        """Common table expressions"""
        return self._input_dataframes_as_cte() + self.queue

    def generate_cte_pipeline_sql(self) -> str:
        self.spent = True

        pipeline = self.ctes_pipeline()

        self._log_pipeline(pipeline)

        with_ctes_pipeline = pipeline[:-1]
        final_query = pipeline[-1]

        with_ctes = [f"{p.output_table_name} as ({p.sql})" for p in with_ctes_pipeline]
        with_ctes_str = ", \n\n".join(with_ctes)
        if with_ctes_str:
            with_ctes_str = f"\nWITH\n\n{with_ctes_str} "

        final_sql = with_ctes_str + "\n" + final_query.sql

        return final_sql

    @property
    def output_table_name(self):
        return self.ctes_pipeline()[-1].output_table_name
