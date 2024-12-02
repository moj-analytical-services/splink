from __future__ import annotations

import json
import pkgutil
import random
import string
from collections import namedtuple
from datetime import datetime, timedelta
from math import ceil, inf, log2
from typing import Iterable

import numpy as np


def dedupe_preserving_order(list_of_items):
    return list(dict.fromkeys(list_of_items))


def prob_to_bayes_factor(prob):
    return prob / (1 - prob) if prob != 1 else inf


def prob_to_match_weight(prob):
    return log2(prob_to_bayes_factor(prob))


def match_weight_to_bayes_factor(weight):
    return 2**weight


def bayes_factor_to_prob(bf):
    return bf / (1 + bf)


def interpolate(start, end, num_elements):
    steps = num_elements - 1
    step = (end - start) / steps
    vals = [start + (i * step) for i in range(0, num_elements)]
    return vals


def normalise(vals):
    return [v / sum(vals) for v in vals]


def ensure_is_iterable(a):
    if isinstance(a, str):
        return [a]
    return a if isinstance(a, Iterable) else [a]


def ensure_is_list(a):
    return a if isinstance(a, list) else [a]


def ensure_is_tuple(a):
    if isinstance(a, tuple):
        return a
    elif isinstance(a, list):
        return tuple(a)
    else:
        return (a,)


def join_list_with_commas_final_and(lst):
    if len(lst) == 1:
        return lst[0]
    return ", ".join(lst[:-1]) + " and " + lst[-1]


class EverythingEncoder(json.JSONEncoder):
    """
    Used to correctly encode data when dumping it to json where we need to
    hardcode json into javascript in a .html file for e.g. comparison viewer

    Without this, json.dumps errors if given an a column of class int32, int64
    np.array, datetime.date etc.

    Thanks to:
    https://github.com/mpld3/mpld3/issues/434#issuecomment-340255689
    """

    # Note that the default method is only called for data types that are
    # NOT natively serializable.  The 'encode' method can be used
    # for natively serializable data
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.bool_):
            return bool(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        try:
            return json.JSONEncoder.default(self, obj)
        except TypeError:
            return obj.__str__()


def calculate_cartesian(df_rows, link_type):
    """
    Calculates the cartesian product for the input df(s).
    """
    n = df_rows

    if link_type == "link_only":
        if len(n) <= 1:
            raise ValueError(
                "if 'link_type 'is 'link_only' should have " "at least two input frames"
            )
        # sum of pairwise product can be found as
        # half of [(sum)-squared - (sum of squares)]
        return (
            sum([m["count"] for m in n]) ** 2 - sum([m["count"] ** 2 for m in n])
        ) / 2

    if link_type == "dedupe_only":
        if len(n) > 1:
            raise ValueError(
                "if 'link_type' is 'dedupe_only' should have only "
                "a single input frame"
            )
        return n[0]["count"] * (n[0]["count"] - 1) / 2

    if link_type == "link_and_dedupe":
        total_rows = sum([m["count"] for m in n])
        return total_rows * (total_rows - 1) / 2

    raise ValueError(
        "'link_type' should be either 'link_only', 'dedupe_only', "
        "or 'link_and_dedupe'"
    )


def major_minor_version_greater_equal_than(this_version, base_comparison_version):
    this_version = this_version.split(".")[:2]
    this_version = [v.zfill(10) for v in this_version]

    base_version = base_comparison_version.split(".")[:2]
    base_version = [v.zfill(10) for v in base_version]

    return this_version >= base_version


def ascii_uid(len):
    # use only lowercase as case-sensitivity is an issue in e.g. postgres
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=len))


def parse_duration(duration: float) -> str:
    # math.ceil to clean up our output for anything over a minute
    seconds = int(ceil(duration))
    if seconds < 60:
        return "{:.5f} seconds".format(duration)

    d = datetime(1, 1, 1) + timedelta(seconds=seconds)
    time_index = namedtuple("time_index", ["Hour", "Minute", "Second"])
    duration_info = time_index(d.hour, d.minute, d.second)

    txt_duration = []
    for t, field in zip(duration_info, duration_info._fields):
        if t == 0:
            continue
        txt = f"{t} {field}s" if t > 1 else f"{t} {field}"
        txt_duration.append(txt)

    if len(txt_duration) > 1:
        # pop off the final bit of text so we can return
        # " and n seconds"
        fin = f" and {txt_duration.pop(-1)}"
        return ", ".join(txt_duration) + fin
    else:
        return txt_duration.pop()


def read_resource(path: str) -> str:
    """Reads a resource file from the splink package"""
    # In the future we may have to move to importlib.resources
    # https://github.com/python/cpython/issues/89838#issuecomment-1093935933
    # But for now this API avoids having to do conditional imports
    # depending on python version.
    # Also, if you use importlib.resources, then you have to add an
    # __init__.py file to every subdirectory, which is annoying.
    if (resource_data := pkgutil.get_data("splink", path)) is None:
        raise FileNotFoundError(f"Could not locate splink resource at: {path}")
    return resource_data.decode("utf-8")


def threshold_args_to_match_weight(
    threshold_match_probability: float | None, threshold_match_weight: float | None
) -> float | None:
    if threshold_match_probability is not None and threshold_match_weight is not None:
        raise ValueError(
            "Cannot provide both threshold_match_probability and "
            "threshold_match_weight. Please specify only one."
        )

    if threshold_match_probability is not None:
        if threshold_match_probability == 0:
            return None
        return prob_to_match_weight(threshold_match_probability)

    if threshold_match_weight is not None:
        return threshold_match_weight

    return None


def threshold_args_to_match_prob(
    threshold_match_probability: float | None, threshold_match_weight: float | None
) -> float | None:
    if threshold_match_probability is not None and threshold_match_weight is not None:
        raise ValueError(
            "Cannot provide both threshold_match_probability and "
            "threshold_match_weight. Please specify only one."
        )

    if threshold_match_probability is not None:
        return threshold_match_probability

    if threshold_match_weight is not None:
        return bayes_factor_to_prob(
            match_weight_to_bayes_factor(threshold_match_weight)
        )

    return None


def threshold_args_to_match_prob_list(
    match_probability_thresholds: list[float] | None,
    match_weight_thresholds: list[float] | None,
) -> list[float] | None:
    if match_probability_thresholds is not None and match_weight_thresholds is not None:
        raise ValueError(
            "Cannot provide both match_probability_thresholds and "
            "match_weight_thresholds. Please specify only one."
        )

    if match_probability_thresholds is not None:
        return sorted(match_probability_thresholds)

    if match_weight_thresholds is not None:
        return sorted(
            bayes_factor_to_prob(match_weight_to_bayes_factor(w))
            for w in match_weight_thresholds
        )

    return None
