from math import log2
from typing import Iterable


def dedupe_preserving_order(list_of_items):
    return list(dict.fromkeys(list_of_items))


def escape_columns(cols):
    return [c.name(escape=True) for c in cols]


def prob_to_bayes_factor(prob):
    return prob / (1 - prob)


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
    return a if isinstance(a, Iterable) else [a]


def ensure_is_list(a):
    return a if isinstance(a, list) else [a]


def join_list_with_commas_final_and(lst):
    if len(lst) == 1:
        return lst[0]
    return ", ".join(lst[:-1]) + " and " + lst[-1]
