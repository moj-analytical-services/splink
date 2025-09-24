from typing import TypeVar

import sqlglot
import sqlglot.expressions as exp


def sqlglot_transform_sql(sql, func, sqlglot_dialect=None):
    syntax_tree = sqlglot.parse_one(sql, read=sqlglot_dialect)
    transformed_tree = syntax_tree.transform(func)
    return transformed_tree.sql(sqlglot_dialect)


def _add_l_or_r_to_identifier(node: exp.Expression) -> exp.Expression:
    if not isinstance(node, exp.Identifier):
        return node

    if (p := node.parent) is None:
        raise TypeError(f"Node {node} has no parent")
    if isinstance(p, (exp.Lambda, exp.Anonymous, exp.ArrayContains)):
        # node is the `x` in the lambda `x -> list_contains(r.name_list, x))`
        parent_table = ""
    else:
        parent_table = p.table  # type: ignore [attr-defined]

    if parent_table != "":
        l_r = "_" + parent_table
    else:
        l_r = ""
    node.args["this"] += l_r
    return node


def _remove_table_prefix(node):
    if isinstance(node, exp.Column):
        n = node.sql().replace(f"{node.table}.", "")
        return sqlglot.parse_one(n)
    return node


def move_l_r_table_prefix_to_column_suffix(
    blocking_rule: str, sqlglot_dialect: str = None
) -> str:
    expression_tree = sqlglot.parse_one(blocking_rule, read=sqlglot_dialect)
    transformed_tree = expression_tree.transform(_add_l_or_r_to_identifier)
    transformed_tree = transformed_tree.transform(_remove_table_prefix)
    return transformed_tree.sql(dialect=sqlglot_dialect)


def add_quotes_and_table_prefix(syntax_tree, table_name):
    """Quotes and adds a table name to your input column(s).

    > tree = sqlglot.parse_one("concat(first_name, surname)")
    > add_quote_and_table_prefix(tree, "l")
    > tree.sql() -> `concat(l."first_name", l."surname")`

    Args:
        syntax_tree (sqlglot.expression): A sqlglot syntax
            tree.
        table_name (str): The table prefix you wish to add
            to all columns.

    Returns:
        sqlglot.expression: A sqlglot syntax tree with quoted
            columns and a table prefix.
    """

    tree = syntax_tree.copy()

    for col in tree.find_all(exp.Column):
        identifier = col.find(exp.Identifier)
        identifier.args["quoted"] = True
        col.args["table"] = table_name

    return tree


def sqlglot_tree_signature_dict(tree_dump):
    def _signature(sub_tree):
        if not isinstance(sub_tree, dict) or "class" not in sub_tree:
            return ""

        child_signatures = [
            child_sig
            for child in sub_tree.get("args", {}).values()
            if (child_sig := _signature(child))
        ]

        if child_signatures:
            return f"{sub_tree['class']}({', '.join(child_signatures)})"
        else:
            return sub_tree["class"]

    return _signature(tree_dump)


def sqlglot_tree_signature_list(dump_list):
    def make_entry(klass):
        return {"class": klass, "children": []}

    nested_dump = make_entry(dump_list[0]["c"])
    dump_dict = {0: nested_dump}
    for index, d in enumerate(dump_list):
        if index == 0:
            continue
        if klass := d.get("c", None):
            entry = make_entry(klass)
            dump_dict[index] = entry
            dump_dict[d["i"]]["children"].append(entry)

    def _signature(sub_tree):
        child_signatures = [
            child_sig
            for child in sub_tree["children"]
            if (child_sig := _signature(child))
        ]

        if child_signatures:
            return f"{sub_tree['class']}({', '.join(child_signatures)})"
        else:
            return sub_tree["class"]

    return _signature(nested_dump)


def sqlglot_tree_signature(sqlglot_tree):
    """A short string representation of a SQLglot tree.

    Allows you to check the type and placement
    of nodes in the AST are as expected.

    e.g. lower(hello) -> Lower(Column(Identifier))"""
    tree_dump = sqlglot_tree.dump()
    # tree_dump may be in different formats depending on the sqlglot version
    # < 27.15.0 is dict format
    # from 27.15.0 it's a list,
    if isinstance(tree_dump, dict):
        return sqlglot_tree_signature_dict(tree_dump)
    if isinstance(tree_dump, list):
        return sqlglot_tree_signature_list(tree_dump)
    raise TypeError(f"sqlglot serialised to an unexpected format: {type(tree_dump)}.")


T = TypeVar("T", bound=exp.Expression)


def remove_quotes_from_identifiers(tree: T) -> T:
    tree = tree.copy()
    for identifier in tree.find_all(exp.Identifier):
        identifier.args["quoted"] = False
    return tree


def add_suffix_to_all_column_identifiers(
    sql_str: str, suffix: str, sqlglot_dialect: str
) -> str:
    """
    Adds a suffix to all column identifiers in the given SQL string.

    Args:
        sql_str (str): The SQL string to transform.
        suffix (str): The suffix to add to each column identifier.
        sqlglot_dialect (str): The SQL dialect used by sqlglot.

    Returns:
        str: The transformed SQL string.

    Examples:
        >>> sql_str = "lower(first_name)"
        >>> add_suffix_to_all_column_identifiers(sql_str, "l", "duckdb")
        'lower(first_name_l)'

        >>> sql_str = "concat(first_name, surname)"
        >>> add_suffix_to_all_column_identifiers(sql_str, "_r", "duckdb")
        'concat(first_name_r, surname_r)'
    """
    tree = sqlglot.parse_one(sql_str, dialect=sqlglot_dialect)

    for col in tree.find_all(exp.Column):
        if (identifier := col.find(exp.Identifier)) is None:
            raise ValueError(f"Failed to find identifier for column {col}")
        identifier.args["this"] = identifier.args["this"] + suffix

    return tree.sql(dialect=sqlglot_dialect)


# TODO: can we get rid of add_quotes_and_table_prefix and use this everywhere instead
def add_table_to_all_column_identifiers(
    sql_str: str, table_name: str, sqlglot_dialect: str
) -> str:
    tree = sqlglot.parse_one(sql_str, dialect=sqlglot_dialect)
    for col in tree.find_all(exp.Column):
        col.args["table"] = table_name
    return tree.sql(dialect=sqlglot_dialect)
