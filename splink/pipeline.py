from copy import deepcopy
import logging

import sqlglot
from sqlglot.expressions import Table

logger = logging.getLogger(__name__)


class SQLTask:
    def __init__(
        self, sql, output_table_name, translates_physical_into_templated=False
    ):
        self.sql = sql
        self.output_table_name = output_table_name

    @property
    def _uses_tables(self):
        tree = sqlglot.parse_one(self.sql, read=None)

        table_names = set()
        for subtree, parent, key in tree.walk():
            if type(subtree) is Table:
                table_names.add(subtree.sql())
        return list(table_names)

    @property
    def _task_description(self):
        uses_tables = ", ".join(self._uses_tables)
        uses_tables = f" {uses_tables} "

        return (
            f"Task reads tables [{uses_tables}]"
            f" and has output table name: {self.output_table_name}"
        )


class SQLPipeline:
    def __init__(self):
        self.queue = []

    def enqueue_sql(self, sql, output_table_name):
        sql_task = SQLTask(sql, output_table_name)
        self.queue.append(sql_task)

    def _generate_pipeline_parts(self, input_dataframes):

        parts = deepcopy(self.queue)
        for df in input_dataframes:
            if not df.physical_and_template_names_equal:
                sql = f"select * from {df.physical_name}"
                task = SQLTask(
                    sql, df.templated_name, translates_physical_into_templated=True
                )
                parts.insert(0, task)
        return parts

    def _log_pipeline(self, parts, input_dataframes):

        if logger.isEnabledFor(7):

            inputs = ", ".join(df.physical_name for df in input_dataframes)
            logger.log(
                7,
                f"SQL pipeline was passed inputs [{inputs}] and output "
                f"dataset {parts[-1].output_table_name}",
            )

            for i, part in enumerate(parts):
                logger.log(7, f"    Pipeline part {i+1}: {part._task_description}")

    def _generate_pipeline(self, input_dataframes):

        parts = self._generate_pipeline_parts(input_dataframes)

        self._log_pipeline(parts, input_dataframes)

        with_parts = parts[:-1]
        last_part = parts[-1]

        with_parts = [f"{p.output_table_name} as ({p.sql})" for p in with_parts]
        with_parts = ", \n".join(with_parts)
        if with_parts:
            with_parts = f"WITH {with_parts} "

        final_sql = with_parts + last_part.sql

        return final_sql

    def reset(self):
        self.queue = []
