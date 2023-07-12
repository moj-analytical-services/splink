import re

import sqlglot


def add_frame_clause(node):
    sql = node.sql()
    sql = re.sub(
        r"(partition by \w+ order by \w+ desc)",
        r"\1 rows between unbounded preceding and current row",
        sql,
        flags=re.IGNORECASE,
    )
    return sqlglot.parse_one(sql)
