import logging
from typing import List, Optional

import sqlglot
from sqlglot.errors import ParseError
from sqlglot.expressions import Table

from .misc import ensure_is_list
from .splink_dataframe import SplinkDataFrame

logger = logging.getLogger(__name__)


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

        table_names = set()
        for subtree, _parent, _key in tree.walk():
            if type(subtree) is Table:
                table_names.add(subtree.sql())
        return list(table_names)

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
            self.input_dataframes = []
        else:
            self.input_dataframes = ensure_is_list(input_dataframes)

    def enqueue_sql(self, sql, output_table_name):
        sql_task = CTE(sql, output_table_name)
        self.queue.append(sql_task)

    def enqueue_list_of_sqls(self, sql_list: List[dict]):
        for sql_dict in sql_list:
            self.enqueue_sql(sql_dict["sql"], sql_dict["output_table_name"])

    def append_input_dataframe(self, df: SplinkDataFrame):
        self.input_dataframes.append(df)

    def _input_dataframes_as_cte(self):
        return [
            CTE(f"\nselect * from {df.physical_name}", df.templated_name)
            for df in self.input_dataframes
            if not df.physical_and_template_names_equal
        ]

    def _log_pipeline(self, parts, input_dataframes: List[SplinkDataFrame]):
        if logger.isEnabledFor(7):
            inputs = ", ".join(df.physical_name for df in input_dataframes)
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

    def generate_cte_pipeline_sql(self, input_dataframes: List[SplinkDataFrame]):
        for df in input_dataframes:
            self.append_input_dataframe(df)

        pipeline = self.ctes_pipeline()

        self._log_pipeline(pipeline, input_dataframes)

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
        return self.queue[-1].output_table_name

    def reset(self):
        self.queue = []
        self.input_dataframes = []
