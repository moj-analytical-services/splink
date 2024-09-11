from typing import Any, Dict

import sqlglot
import sqlglot.expressions
from sqlglot.expressions import Expression

from splink.internals.comparison_creator import ComparisonCreator
from splink.internals.comparison_level_creator import ComparisonLevelCreator
from splink.internals.database_api import (
    DatabaseAPISubClass,
)
from splink.internals.pipeline import CTEPipeline


def _set_quoted(node: Expression) -> Expression:
    if hasattr(node, "quoted"):
        node.set("quoted", True)
    return node


def _replace_identifier(node, replacements: Dict[str, Any], dialect: str):
    """Replace identifiers in a sqlglot node with literal values.

    Args:
        node: A sqlglot node
        replacements: A dictionary mapping column names to their literal values
        dialect: The SQL dialect to use for parsing

    Returns:
        A new sqlglot node with identifiers replaced by literal values
    """
    node_quoted = node.transform(lambda x: _set_quoted(x))
    for key, replacement in replacements.items():
        parsed_key = sqlglot.parse_one(key, dialect=dialect)

        parsed_key_quoted = parsed_key.transform(lambda x: _set_quoted(x))

        if node_quoted == parsed_key_quoted:
            if replacement is None:
                return sqlglot.exp.null()
            elif isinstance(replacement, str):
                return sqlglot.exp.Literal(this=replacement, is_string=True)
            else:
                return sqlglot.exp.Literal(this=f"{replacement}", is_string=False)

    return node


def is_in_level(
    comparison_level: ComparisonLevelCreator,
    literal_values: Dict[str, Any],
    db_api: DatabaseAPISubClass,
) -> bool:
    """Check if a set of literal values satisfies a comparison level condition.

    Args:
        comparison_level: A ComparisonLevelCreator object
        literal_values: A dictionary mapping column names to their literal values
        db_api: A DatabaseAPI object for executing SQL queries

    Returns:
        Whether the literal values satisfy the comparison level condition
    """
    sqlglot_dialect = db_api.sql_dialect.sqlglot_name
    sql_cond = comparison_level.get_comparison_level(sqlglot_dialect).sql_condition
    if sql_cond == "ELSE":
        return True

    tree = sqlglot.parse_one(sql_cond, dialect=sqlglot_dialect)

    new_tree = tree.transform(
        lambda node: _replace_identifier(node, literal_values, sqlglot_dialect)
    )
    sql_to_evaluate = new_tree.sql(dialect=sqlglot_dialect)
    sql_to_evaluate = f"SELECT {sql_to_evaluate} as result"

    pipeline = CTEPipeline()
    pipeline.enqueue_sql(sql_to_evaluate, "__splink__is_in_level")
    res = db_api.sql_pipeline_to_splink_dataframe(pipeline)
    return bool(res.as_record_dict()[0]["result"])


def compute_comparison_vector_value(
    comparison: ComparisonCreator,
    literal_values: Dict[str, Any],
    db_api: DatabaseAPISubClass,
) -> Dict[str, Any]:
    """Compute the comparison vector value for a set of literal values.

    Args:
        comparison: A ComparisonCreator object
        literal_values: A dictionary mapping column names to their literal values
        db_api: A DatabaseAPI object for executing SQL queries

    Returns:
        A dictionary containing the comparison vector value and label for charts
    """
    sqlglot_dialect = db_api.sql_dialect.sqlglot_name

    creator_levels = comparison.create_comparison_levels()

    instantiated_levels = comparison.get_comparison(sqlglot_dialect).comparison_levels

    levels = [
        {
            "creator": c,
            "comparison_vector_value": inst.comparison_vector_value,
            "label_for_charts": inst.label_for_charts,
        }
        for c, inst in zip(creator_levels, instantiated_levels)
    ]

    for d in levels:
        in_level = is_in_level(d["creator"], literal_values, db_api)
        if in_level:
            return {
                "comparison_vector_value": d["comparison_vector_value"],
                "label_for_charts": d["label_for_charts"],
            }

    return {}
