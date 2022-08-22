import sqlglot
import sqlglot.expressions as exp


def transformer_base(func):
    def wrapper(sql, *args, **kwargs):
        """
        Args:
            sql Union(str or sqlglot.SyntaxTree): If a sqlglot object
                is passed to the transformer, it is simply given to the
                transformer and the resulting sql is returned.
                If a sql expression is passed, this will be parsed by the
                sqlglot parser and the resulting syntax tree will be transformed.
        """

        if isinstance(sql, exp.Expression):
            syntax_tree = sql
        else:
            syntax_tree = sqlglot.parse_one(sql, read=None)
        transformed_tree = syntax_tree.transform(func, *args, **kwargs)
        return transformed_tree.sql()

    return wrapper


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


@transformer_base
def add_prefix_or_suffix_to_colname(node, escape=True, prefix="", suffix=""):
    if isinstance(node, exp.Column):
        node.this.args["this"] = f"{prefix}{node.sql()}{suffix}"
        if escape:
            node.this.args["quoted"] = True

    return node


@transformer_base
def cast_concat_as_varchar(node):
    if isinstance(node, exp.Column):

        if isinstance(node.parent, exp.Cast):
            return node

        if node.find_ancestor(exp.DPipe):
            sql = f"cast({node.sql()} as varchar)"
            return sqlglot.parse_one(sql)

    return node
