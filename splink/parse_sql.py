import sqlglot
from sqlglot.expressions import Column, Bracket, Lambda


def get_columns_used_from_sql(sql, dialect="spark", retain_table_prefix=False):
    column_names = set()
    syntax_tree = sqlglot.parse_one(sql, read=dialect)
    path = {}
    for tup in syntax_tree.walk():
        subtree = tup[0]
        if hasattr(subtree, "depth"):
            path[subtree.depth] = type(subtree)
        if Lambda in path.values():
            continue
        if type(subtree) in (Column, Bracket):
            if retain_table_prefix:
                column_names.add(subtree.sql())
            else:
                column_names.add(subtree.this.sql())
    return list(column_names)
