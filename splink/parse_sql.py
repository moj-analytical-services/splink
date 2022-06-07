import sqlglot
from sqlglot.expressions import Column, Bracket, Lambda


def get_columns_used_from_sql(sql, dialect=None, retain_table_prefix=False):
    column_names = set()
    syntax_tree = sqlglot.parse_one(sql, read=dialect)
    for tup in syntax_tree.walk():
        subtree = tup[0]

        if type(subtree) in (Column, Bracket):

            # check if any parents are lambdas
            parent = subtree.parent
            while parent is not None:
                if type(parent) == Lambda:
                    break
                parent = parent.parent

            if type(parent) == Lambda:
                continue

            if subtree.find(Bracket) and type(subtree) == Column:
                # Column with bracket in it
                table = subtree.table
                column = subtree.this.this.this
            elif type(subtree.parent) != Column and type(subtree) == Column:
                # Plain column
                table = subtree.table

                column = subtree.this.this
            else:
                # Plain bracket
                table = None
                column = subtree.this.this.this

            if retain_table_prefix and table is not None:
                column_names.add(f"{table.this}.{column}")
            else:
                column_names.add(column)

    return list(column_names)
