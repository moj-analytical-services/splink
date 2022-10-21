from splink.duckdb.duckdb_linker import DuckDBLinker
from splink.misc import calculate_cartesian

import pandas as pd


# TODO: (somwehere, maybe not here) cross-check calculation with the blocked frame we get using rule 1=1

# convenience to get list into format as though it was result of a count query
def list_to_row_count(l):
    return [{"count": el} for el in l]


def test_calculate_cartesian_dedupe_only():
    # dedupe_only - have n(n-1)/2 comparisons, only a single frame
    assert calculate_cartesian(list_to_row_count([5]), "dedupe_only") == 10
    assert calculate_cartesian(list_to_row_count([8]), "dedupe_only") == 28
    assert calculate_cartesian(list_to_row_count([10]), "dedupe_only") == 45
    # TODO: define behaviour for n > 1 tables? error? would also need to think about n = 1 for link_only


def test_calculate_cartesian_link_only():
    # link_only - sum of all pairwise comparisons - i.e. sum of all pairwise products
    assert calculate_cartesian(list_to_row_count([2, 3]), "link_only") == 6
    assert calculate_cartesian(list_to_row_count([7, 11]), "link_only") == 77
    assert calculate_cartesian(list_to_row_count([2, 2, 2]), "link_only") == 12
    assert calculate_cartesian(list_to_row_count([2, 3, 5]), "link_only") == 31
    assert calculate_cartesian(list_to_row_count([1, 1, 1]), "link_only") == 3
    assert calculate_cartesian(list_to_row_count([2, 2, 2, 2, 2]), "link_only") == 40
    assert calculate_cartesian(list_to_row_count([5, 5, 5, 5]), "link_only") == 150


def test_calculate_cartesian_link_and_dedupe():
    # link_and_dedupe - much like dedupe, N(N - 1)/2 comparisons with N = sum of rows of all frames
    # alternatively can think of this as 'link only' links + dedupe links for each frame in list
    assert calculate_cartesian(list_to_row_count([8]), "link_and_dedupe") == 28
    assert calculate_cartesian(list_to_row_count([2, 3]), "link_and_dedupe") == 10
    assert calculate_cartesian(list_to_row_count([7, 11]), "link_and_dedupe") == 77 + 21 + 55
    assert calculate_cartesian(list_to_row_count([2, 2, 2]), "link_and_dedupe") == 15
    assert calculate_cartesian(list_to_row_count([1, 1, 1]), "link_and_dedupe") == 3
    assert calculate_cartesian(list_to_row_count([2, 2, 2, 2, 2]), "link_only") == 45
    assert calculate_cartesian(list_to_row_count([5, 5, 5, 5]), "link_only") == 190
