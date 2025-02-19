import sqlglot


def execute_sql_logging_message_info(templated_name, physical_name):
    return (
        f"Executing sql to create "
        f"{templated_name} "
        f"as physical name {physical_name}"
    )


def log_sql(sql):
    return (
        "\n------Start SQL---------\n"
        f"{sqlglot.parse_one(sql, dialect='spark').sql(pretty=True)}\n"
        "-------End SQL-----------\n"
    )
