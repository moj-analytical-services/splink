from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from jinja2 import StrictUndefined, Template, UndefinedError

logger = logging.getLogger(__name__)

# https://stackoverflow.com/questions/39740632/python-type-hinting-without-cyclic-imports
if TYPE_CHECKING:
    from .linker import Linker


class PartialJinjaTemplate:
    def __init__(self, template_str):
        self.template = Template(template_str, undefined=StrictUndefined)
        self.values = {}

    def add_value(self, key, value):
        self.values[key] = value

    def render(self):
        try:
            return self.template.render(self.values)
        except UndefinedError as e:
            raise ValueError(f"Template rendering failed. Missing value for: {str(e)}")


def vertically_concatenate_sql(linker: Linker) -> PartialJinjaTemplate:
    template = """
    {% if source_dataset_col_req %}
        {% for df_obj in input_tables %}
            select
            {% if not source_dataset_column_already_exists %}
                '{{ df_obj.templated_name }}' as source_dataset,
            {% endif %}
            {{ ", ".join(columns) }}
            {% if salting_required %}
                , random() as __splink_salt
            {% endif %}
            from {{ df_obj.physical_name }}
            {% if not loop.last %}
                UNION ALL
            {% endif %}
        {% endfor %}
    {% else %}
        select {{ ", ".join(columns) }}
        {% if salting_required %}
            , random() as __splink_salt
        {% endif %}
        from {{ df_obj.physical_name }}
    {% endif %}
    """

    partial_template = PartialJinjaTemplate(template)

    df_obj = next(iter(linker._input_tables_dict.values()))
    columns = df_obj.columns_escaped
    input_tables = list(linker._input_tables_dict.values())

    if linker._settings_obj_ is None:
        source_dataset_col_req = True
        salting_required = False
    else:
        source_dataset_col_req = (
            linker._settings_obj._source_dataset_column_name_is_required
        )
        salting_required = linker._settings_obj.salting_required

    # Overriding salting_required if the SQL dialect is DuckDB
    if linker._sql_dialect == "duckdb":
        salting_required = True

    source_dataset_column_already_exists = linker._source_dataset_column_already_exists

    # Adding values to the partial template
    partial_template.add_value("source_dataset_col_req", source_dataset_col_req)
    partial_template.add_value("input_tables", input_tables)
    partial_template.add_value("columns", columns)
    partial_template.add_value("salting_required", salting_required)
    partial_template.add_value(
        "source_dataset_column_already_exists", source_dataset_column_already_exists
    )
    partial_template.add_value("df_obj", df_obj)  # Add this line

    sql = partial_template.render()
    print(sql)
    return sql
