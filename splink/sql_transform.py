import sqlglot
import sqlglot.expressions as exp


def sqlglot_transform_sql(sql, func, dialect=None):
    syntax_tree = sqlglot.parse_one(sql, read=dialect)
    transformed_tree = syntax_tree.transform(func)
    return transformed_tree.sql(dialect)


def _add_l_or_r_to_identifier(node: exp.Expression):
    if not isinstance(node, exp.Identifier):
        return node

    p = node.parent
    if isinstance(p, (exp.Lambda, exp.Anonymous)):
        # node is the `x` in the lambda `x -> list_contains(r.name_list, x))`
        parent_table = ""
    else:
        parent_table = p.table

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


def move_l_r_table_prefix_to_column_suffix(blocking_rule):
    expression_tree = sqlglot.parse_one(blocking_rule, read=None)
    transformed_tree = expression_tree.transform(_add_l_or_r_to_identifier)
    transformed_tree = transformed_tree.transform(_remove_table_prefix)
    return transformed_tree.sql()


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


def sqlglot_tree_signature(sqlglot_tree):
    """A short string representation of a SQLglot tree.

    Allows you to check the type and placement
    of nodes in the AST are as expected.

    e.g. lower(hello) -> Lower(Column(Identifier))"""

    def _signature(sub_tree):
        if not isinstance(sub_tree, dict) or "class" not in sub_tree:
            return ""

        child_signatures = [
            _signature(child)
            for child in sub_tree.get("args", {}).values()
            if _signature(child)
        ]

        if child_signatures:
            return f"{sub_tree['class']}({', '.join(child_signatures)})"
        else:
            return sub_tree["class"]

    return _signature(sqlglot_tree.dump())


def remove_quotes_from_identifiers(tree) -> exp.Expression:
    tree = tree.copy()
    for identifier in tree.find_all(exp.Identifier):
        identifier.args["quoted"] = False
    return tree
