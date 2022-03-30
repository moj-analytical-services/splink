def execute_sql_logging_message_info(templated_name, physical_name):
    return (
        f"Executing sql with templated output tablename: "
        f"{templated_name} "
        f"and physical name {physical_name}"
    )
