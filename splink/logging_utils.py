import logging

sqlparse_exists = True
try:
    import sqlparse
except ImportError:
    sqlparse_exists = False
from textwrap import dedent

def _format_sql(sql):
    if sqlparse_exists:
        return sqlparse.format(sql, reindent=True, keyword_case='upper')
    else:
        return dedent(sql)


