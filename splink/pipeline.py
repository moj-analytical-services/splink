from copy import deepcopy


class SQLTask:
    def __init__(self, sql, output_table_name):
        self.sql = sql
        self.output_table_name = output_table_name


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
                task = SQLTask(sql, df.templated_name)
                parts.insert(0, task)
        return parts

    def _generate_pipeline(self, input_dataframes):

        parts = self._generate_pipeline_parts(input_dataframes)

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
