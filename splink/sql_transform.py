import sqlglot
import sqlglot.expressions as exp


def sqlglot_transform_sql(sql, func):
    syntax_tree = sqlglot.parse_one(sql, read=None)
    transformed_tree = syntax_tree.transform(func)
    return transformed_tree.sql()


def _add_l_or_r_to_identifier(node):
    if isinstance(node, exp.Identifier):
        if isinstance(node.parent, exp.Bracket):
            l_r = node.parent.parent.table
        else:
            l_r = node.parent.table
        node.args["this"] += f"_{l_r}"
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
