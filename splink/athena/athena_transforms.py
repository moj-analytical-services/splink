import sqlglot
from sqlglot import expressions as exp


def cast_concat_as_varchar(node):
    if isinstance(node, exp.Column):

        if isinstance(node.parent, exp.Cast):
            return node

        if node.find_ancestor(exp.DPipe):
            sql = f"cast({node.sql()} as varchar)"
            return sqlglot.parse_one(sql)

    return node
