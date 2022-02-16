from .validate_jsonschema import get_schema


def default_value_from_schema(key, schema_part):
    schema = get_schema()
    if schema_part == "root":
        return schema["properties"][key]["default"]

    if schema_part == "comparison":
        cc = schema["properties"]["comparisons"]
        return cc["items"]["properties"][key]["default"]

    if schema_part == "comparison_level":
        cc = schema["properties"]["comparisons"]
        cl = cc["items"]["properties"]["comparison_levels"]
        return cl["items"]["properties"][key]["default"]

    return None
