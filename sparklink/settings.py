

def _get_columns_to_retain(settings):
    columns_to_retain = [settings["unique_id_column_name"]]
    columns_to_retain = columns_to_retain + [c["col_name"] for c in settings["comparison_columns"]]
    columns_to_retain = columns_to_retain + settings["additional_columns_to_retain"]
    return columns_to_retain