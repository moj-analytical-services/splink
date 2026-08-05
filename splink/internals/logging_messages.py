def execute_sql_logging_message_info(templated_name, physical_name):
    return f"Executing sql to create {templated_name} as physical name {physical_name}"


def log_sql(sql):
    return f"\n------Start SQL---------\n{sql}\n-------End SQL-----------\n"
