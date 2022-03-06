import json
from functools import lru_cache
from functools import reduce
import operator
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

    def getFromDict(dataDict, mapList):
        return reduce(operator.getitem, mapList, dataDict)

    if e:
        path = list(e.path)

        comparison_level = None

        try:
            index_of_comparison_levels = path.index("comparison_levels")
        except ValueError:
            index_of_comparison_levels = None
        if index_of_comparison_levels is not None:
            comparison_level = getFromDict(
                settings_dict, path[: index_of_comparison_levels + 2]
            )

        comparison = None
        try:
            index_of_comparison = path.index("comparisons")
        except ValueError:
            index_of_comparison = None
        if index_of_comparison is not None:
            comparison = getFromDict(settings_dict, path[: index_of_comparison + 2])

        error_in = ""
        if comparison_level:
            error_in += f"The comparison level is: {json.dumps(comparison_level)}\n\n"

        if comparison:
            error_in += f"The comparison is: {json.dumps(comparison)}\n\n"

        if error_in == "":
            error_in = (
                "The error is in the main settings object, not in the "
                "comparison columns or levels."
            )

        message = (
            f"There was at least one error in your settings dictionary.\n\n"
            f"The first error was:   {e.message}\n\n"
            f"The path to the error is:\n     {json.dumps(path)}\n\n"
            f"The part of your settings dictionary containing this error is:\n"
            f"{json.dumps(e.instance)}\n"
            f"{error_in}\n"
        )
        raise ValueError(message)
