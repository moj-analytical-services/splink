from __future__ import annotations

import json
import operator
from functools import lru_cache, reduce

from splink.internals.misc import read_resource


@lru_cache()
def get_schema():
    path = "files/settings_jsonschema.json"
    return json.loads(read_resource(path))


def get_from_dict(dataDict, mapList):
    return reduce(operator.getitem, mapList, dataDict)


def get_comparison_level(e, settings_dict):
    comparison_level = None
    path = list(e.path)

    try:
        index_of_comparison_levels = path.index("comparison_levels")
    except ValueError:
        index_of_comparison_levels = None
    if index_of_comparison_levels is not None:
        comparison_level = get_from_dict(
            settings_dict, path[: index_of_comparison_levels + 2]
        )
    return comparison_level


def get_comparison(e, settings_dict):
    path = list(e.path)
    comparison = None
    try:
        index_of_comparison = path.index("comparisons")
    except ValueError:
        index_of_comparison = None
    if index_of_comparison is not None:
        comparison = get_from_dict(settings_dict, path[: index_of_comparison + 2])
    return comparison
