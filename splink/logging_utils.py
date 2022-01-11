from textwrap import dedent
import sqlglot


def _format_sql(sql):
    try:
        parsed_list = sqlglot.parse(sql, read="spark")
        return_sql = [p.sql(dialect="spark", pretty=True) for p in parsed_list]
        return "\n".join(return_sql)

    except:
        return dedent(sql)
