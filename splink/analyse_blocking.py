from typing import TYPE_CHECKING

from .blocking import _sql_gen_where_condition
from .settings import Settings
from .comparison_library import exact_match

# https://stackoverflow.com/questions/39740632/python-type-hinting-without-cyclic-imports
if TYPE_CHECKING:
    from .linker import Linker


def number_of_comparisons_generated_by_blocking_rule_sql(
    linker: "Linker", blocking_rule, link_type=None
) -> str:

    if linker._settings_obj_ is not None:
        settings_obj = linker._settings_obj

    if link_type is None and linker._settings_obj_ is None:
        if len(linker._input_tables_dict.values()) == 1:
            link_type = "dedupe_only"

    if link_type is not None:
        # Minimal settings dict
        settings_obj = Settings(
            {"link_type": link_type, "comparisons": [exact_match("first_name")]}
        )

    # If link type not specified or inferrable, raise error
    if link_type is None:
        if linker._settings_obj_ is None:
            raise ValueError(
                "Must provide a link_type argument to analyse_blocking_rule_sql "
                "if linker has no settings object"
            )

    where_condition = _sql_gen_where_condition(
        settings_obj._link_type, settings_obj._unique_id_input_columns
    )

    sql = f"""
    select count(*) as count_of_pairwise_comparisons_generated

    from __splink__df_concat as l
    inner join __splink__df_concat as r
    on
    {blocking_rule}
    {where_condition}
    """

    return sql
