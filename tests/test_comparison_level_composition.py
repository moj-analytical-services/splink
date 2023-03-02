import pandas as pd
import pytest

import splink.duckdb.duckdb_comparison_level_library as cll
from splink.duckdb.duckdb_linker import DuckDBLinker


def test_composition_internals():
    composition_funs = {
        "OR": cll.cl_or,
        "AND": cll.cl_and,
    }

    for clause, c_fun in composition_funs.items():

        # Two null levels composed
        level = c_fun(
            cll.null_level("first_name"),
            cll.null_level("surname"),
            label_for_charts="This is a test",
        ).__dict__

        null_sql = f'("first_name_l" IS NULL OR "first_name_r" IS NULL) {clause} ' \
                    '("surname_l" IS NULL OR "surname_r" IS NULL)'
        assert level["_level_dict"]["sql_condition"] == null_sql
        # Default label
        assert level["_level_dict"]["label_for_charts"] == "This is a test"
        assert level["_level_dict"]["is_null_level"] is True

        # Exact match and null level composition
        level = c_fun(
            cll.exact_match_level("first_name"),
            cll.null_level("first_name"),
            m_probability=0.5,
        ).__dict__
        assert (
            level["_level_dict"]["sql_condition"]
            == f'("first_name_l" = "first_name_r") {clause} ' \
            '("first_name_l" IS NULL OR "first_name_r" IS NULL)'
        )
        # Default label
        assert (
            level["_level_dict"]["label_for_charts"]
            == f"(Exact match on first_name) {clause} (first_name is NULL)"
        )
        # should default to None
        assert level["_level_dict"].get("is_null_level") is None
        assert level["_m_probability"] == 0.5


        # cll.cl_not(cl_or(...)) composition
        level = cll.cl_not(
            c_fun(
                cll.exact_match_level("first_name"),
                cll.exact_match_level("surname")),
            m_probability=0.5,
        ).__dict__

        exact_match_sql = f'("first_name_l" = "first_name_r") {clause} ' \
        '("surname_l" = "surname_r")'
        assert level["_level_dict"]["sql_condition"] == f"NOT ({exact_match_sql})"

        # Check it fails when no inputs are added
        with pytest.raises(ValueError):
            c_fun()


def test_composition_outputs():
    # Check our compositions give expected outputs
    df = pd.DataFrame(
        [
            {
                "unique_id": 1,
                "forename": "Tom",
                "surname": "Tim",
            },
            {
                "unique_id": 2,
                "forename": "Tom",
                "surname": "Tim",
            },
            {
                "unique_id": 3,
                "forename": "Tom",
                "surname": "Timothee",
            },
            {
                "unique_id": 4,
                "forename": "Sam",
                "surname": "Tarly",
            },
            {
                "unique_id": 5,
                "forename": "Sam",
                "surname": "Tim",
            },
        ]
    )

    # For testing the cll version
    full_name = {
        "output_column_name": "full_name",
        "comparison_levels": [
            cll.cl_or(cll.null_level("forename"), cll.null_level("surname")),
            cll.cl_and(
                cll.exact_match_level("forename"), cll.exact_match_level("surname")
            ),
            cll.cl_or(
                cll.exact_match_level("forename"), cll.exact_match_level("surname")
            ),
            cll.cl_not(  # acts as an "else" level
                cll.cl_and(
                    cll.exact_match_level("forename"), cll.exact_match_level("surname")
                )
            ),
            cll.else_level(),
        ],
    }

    settings = {
        "link_type": "dedupe_only",
        "comparisons": [full_name],
    }

    linker = DuckDBLinker(df, settings)

    pred = linker.predict()
    out = pred.as_pandas_dataframe().sort_values(by=["unique_id_l", "unique_id_r"])

    # Check individual IDs are assigned to the correct gamma values
    # Dict key: {gamma_value: tuple of ID pairs}
    size_gamma_lookup = {
        3: {"id_pairs": [(1, 2)]},
        2: {"id_pairs": [(1, 3), (1, 5), (2, 3), (2, 5), (4, 5)]},
        1: {"id_pairs": [(1, 4), (2, 4), (3, 4), (3, 5)]},
    }

    for k, v in size_gamma_lookup.items():
        for ids in v["id_pairs"]:
            assert (
                out.loc[(out.unique_id_l == ids[0]) & (out.unique_id_r == ids[1])][
                    "gamma_full_name"
                ].values[0]
                == k
            )
