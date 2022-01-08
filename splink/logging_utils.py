from textwrap import dedent

import sqlglot

sqlparse_exists = True
try:
    import sqlparse
except ImportError:
    sqlparse_exists = False


def _format_sql(sql):
    if sqlparse_exists:
        return sqlparse.format(sql, reindent=True, keyword_case="upper")

    # SQLGlot current does not support numeric literals like 0.23D or 1Y
    # Formattting will fail for any statements which use these
    try:
        parsed_list = sqlglot.parse(sql)

        return_sql = [p.sql(dialect="spark", pretty=True) for p in parsed_list]
        return "\n".join(return_sql)

    except:
        return dedent(sql)
