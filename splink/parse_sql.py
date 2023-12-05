import sqlglot
import sqlglot.expressions as exp
from sqlglot.expressions import Bracket, Column, Lambda

from .sql_transform import remove_quotes_from_identifiers


def get_columns_used_from_sql(sql, dialect=None, retain_table_prefix=False):
    column_names = set()
    syntax_tree = sqlglot.parse_one(sql, read=dialect)

    for subtree in syntax_tree.find_all(exp.Column):
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

        if retain_table_prefix and table:
            column_names.add(f"{table}.{column}")
        else:
            column_names.add(column)

    return list(column_names)


def parse_columns_in_sql(
    sql: str, sql_dialect: str, remove_quotes=True
) -> sqlglot.Expression:
    """Extract all columns found within a SQL expression.

    Args:
        sql (str): A SQL string you wish to parse.

    Returns:
        list[exp.Column]: A list of columns as SQLglot expressions. These can be
            unwrapped with `.sql()`. If the input string is unparseable, None will
            be returned.
    """
    try:
        syntax_tree = sqlglot.parse_one(sql, read=sql_dialect)
    except Exception:  # Consider catching a more specific exception if possible
        # If we can't parse a SQL condition, it's better to just pass.
        return None

    return [
        # Remove quotes if requested by the user
        remove_quotes_from_identifiers(col) if remove_quotes else col
        for col in syntax_tree.find_all(exp.Column)
    ]
