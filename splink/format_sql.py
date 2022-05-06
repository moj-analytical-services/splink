import sqlglot


def format_sql(sql):

    parsed_list = sqlglot.parse(sql, read=None)
    return_sql = [p.sql(dialect="spark", pretty=True) for p in parsed_list]
    return "\n".join(return_sql)
