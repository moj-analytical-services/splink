import json
from functools import lru_cache

import pkg_resources
from jsonschema import Draft7Validator


@lru_cache()
def get_schema():
    schema_loc = "files/settings_jsonschema.json"
    with pkg_resources.resource_stream(__name__, schema_loc) as io:
        return json.load(io)


def validate_settings_against_schema(settings_dict: dict):
    """Validate a splink settings object against its jsonschema"""

    schema = get_schema()

    v = Draft7Validator(schema)

    e = next(v.iter_errors(settings_dict), None)

    if e:
        path = e.schema_path
        if "comparison_levels" in path:
            error_in = "The error is in one of the comparison levels."
        elif "comparisons" in path:
            error_in = "The error is a problem with a comparison column."
        else:
            error_in = "The error is in the main settings object, not in the comparison columns or levels."

        message = (
            f"There was at least one error in your settings dictionary.\n"
            f"The first error was:   {e.message}\n"
            f"{error_in}\n"
            f"The part of your settings dictionary containing this error is:\n     {json.dumps(e.instance)}"
        )
        raise ValueError(message)
