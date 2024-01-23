import pandas as pd

import splink.comparison_level_library as cll

from .decorator import mark_with_dialects_excluding


@mark_with_dialects_excluding()
def test_column_reversal(test_helpers, dialect):
    helper = test_helpers[dialect]

    data = [
        {"id": 1, "forename": "John", "surname": "Smith", "full_name": "John Smith"},
        {"id": 2, "forename": "Smith", "surname": "John", "full_name": "Smith John"},
        {"id": 3, "forename": "Rob", "surname": "Jones", "full_name": "Rob Jones"},
        {"id": 4, "forename": "Rob", "surname": "Jones", "full_name": "Rob Jones"},
    ]

    settings = {
        "unique_id_column_name": "id",
        "link_type": "dedupe_only",
        "blocking_rules_to_generate_predictions": [],
        "comparisons": [
            {
                "output_column_name": "full_name",
                "comparison_levels": [
                    cll.NullLevel("full_name"),
                    cll.ExactMatchLevel("full_name"),
                    cll.ColumnsReversedLevel("forename", "surname"),
                    cll.ElseLevel(),
                ],
            },
        ],
        "retain_matching_columns": True,
        "retain_intermediate_calculation_columns": True,
    }

    df = pd.DataFrame(data)
    df = helper.convert_frame(df)

    linker = helper.Linker(df, settings, **helper.extra_linker_args())
    df_e = linker.predict().as_pandas_dataframe()

    row = dict(df_e.query("id_l == 1 and id_r == 2").iloc[0])
    assert row["gamma_full_name"] == 1

    row = dict(df_e.query("id_l == 3 and id_r == 4").iloc[0])
    assert row["gamma_full_name"] == 2


@mark_with_dialects_excluding()
def test_perc_difference(test_helpers, dialect):
    helper = test_helpers[dialect]

    data = [
        {"id": 1, "amount": 1.2},
        {"id": 2, "amount": 1.0},
        {"id": 3, "amount": 0.8},
        {"id": 4, "amount": 200},
        {"id": 5, "amount": 140},
    ]

    settings = {
        "unique_id_column_name": "id",
        "link_type": "dedupe_only",
        "blocking_rules_to_generate_predictions": [],
        "comparisons": [
            {
                "output_column_name": "amount",
                "comparison_levels": [
                    cll.NullLevel("amount"),
                    cll.PercentageDifferenceLevel("amount", 0.0),  # 4
                    cll.PercentageDifferenceLevel("amount", (0.2 / 1.2) + 1e-4),  # 3
                    cll.PercentageDifferenceLevel("amount", (0.2 / 1.0) + 1e-4),  # 2
                    cll.PercentageDifferenceLevel("amount", (60 / 200) + 1e-4),  # 1
                    cll.ElseLevel(),
                ],
            },
        ],
        "retain_matching_columns": True,
        "retain_intermediate_calculation_columns": True,
    }

    df = pd.DataFrame(data)
    df = helper.convert_frame(df)

    linker = helper.Linker(df, settings, **helper.extra_linker_args())
    df_e = linker.predict().as_pandas_dataframe()

    row = dict(df_e.query("id_l == 1 and id_r == 2").iloc[0])  # 16.66%
    assert row["gamma_amount"] == 3

    row = dict(df_e.query("id_l == 2 and id_r == 3").iloc[0])  # 20%
    assert row["gamma_amount"] == 2

    row = dict(df_e.query("id_l == 4 and id_r == 5").iloc[0])  # 30%
    assert row["gamma_amount"] == 1


@mark_with_dialects_excluding()
def test_levenshtein_level(test_helpers, dialect):
    helper = test_helpers[dialect]

    data = [
        {"id": 1, "name": "harry"},
        {"id": 2, "name": "harry"},
        {"id": 3, "name": "barry"},
        {"id": 4, "name": "gary"},
        {"id": 5, "name": "sally"},
        {"id": 6, "name": "sharry"},
        {"id": 7, "name": "haryr"},
        {"id": 8, "name": "ahryr"},
        {"id": 9, "name": "harry12345"},
        {"id": 10, "name": "ahrryt"},
        {"id": 11, "name": "hy"},
        {"id": 12, "name": "r"},
    ]
    # id and expected levenshtein distance from id:1 "harry"
    id_distance_from_1 = {
        2: 0,
        3: 1,
        4: 2,
        5: 3,
        6: 1,
        7: 2,
        8: 3,
        9: 5,
        10: 3,
        11: 3,
        12: 4,
    }

    settings = {
        "unique_id_column_name": "id",
        "link_type": "dedupe_only",
        "comparisons": [
            {
                "output_column_name": "name",
                "comparison_levels": [
                    cll.NullLevel("name"),
                    cll.LevenshteinLevel("name", 0),  # 4
                    cll.LevenshteinLevel("name", 1),  # 3
                    cll.LevenshteinLevel("name", 2),  # 2
                    cll.LevenshteinLevel("name", 3),  # 1
                    cll.ElseLevel(),  # 0
                ],
            },
        ],
        "retain_matching_columns": True,
        "retain_intermediate_calculation_columns": True,
    }

    def gamma_lev_from_distance(dist):
        # which gamma value will I get for a given levenshtein distance?
        if dist == 0:
            return 4
        elif dist == 1:
            return 3
        elif dist == 2:
            return 2
        elif dist == 3:
            return 1
        elif dist > 3:
            return 0
        raise ValueError(f"Invalid distance supplied ({dist})")

    df = pd.DataFrame(data)
    df = helper.convert_frame(df)

    linker = helper.Linker(df, settings, **helper.extra_linker_args())
    df_e = linker.predict().as_pandas_dataframe()

    for id_r, lev_dist in id_distance_from_1.items():
        expected_gamma_lev = gamma_lev_from_distance(lev_dist)
        row = dict(df_e.query(f"id_l == 1 and id_r == {id_r}").iloc[0])
        assert row["gamma_name"] == expected_gamma_lev


# postgres has no Damerau-Levenshtein
@mark_with_dialects_excluding("postgres")
def test_damerau_levenshtein_level(test_helpers, dialect):
    helper = test_helpers[dialect]

    data = [
        {"id": 1, "name": "harry"},
        {"id": 2, "name": "harry"},
        {"id": 3, "name": "barry"},
        {"id": 4, "name": "gary"},
        {"id": 5, "name": "sally"},
        {"id": 6, "name": "sharry"},
        {"id": 7, "name": "haryr"},
        {"id": 8, "name": "ahryr"},
        {"id": 9, "name": "harry12345"},
        {"id": 10, "name": "ahrryt"},
        {"id": 11, "name": "hy"},
        {"id": 12, "name": "r"},
    ]
    # id and expected levenshtein distance from id:1 "harry"
    id_distance_from_1 = {
        2: 0,
        3: 1,
        4: 2,
        5: 3,
        6: 1,
        7: 1,
        8: 2,
        9: 5,
        10: 2,
        11: 3,
        12: 4,
    }

    settings = {
        "unique_id_column_name": "id",
        "link_type": "dedupe_only",
        "comparisons": [
            {
                "output_column_name": "name",
                "comparison_levels": [
                    cll.NullLevel("name"),
                    cll.DamerauLevenshteinLevel("name", 0),  # 4
                    cll.DamerauLevenshteinLevel("name", 1),  # 3
                    cll.DamerauLevenshteinLevel("name", 2),  # 2
                    cll.DamerauLevenshteinLevel("name", 3),  # 1
                    cll.ElseLevel(),  # 0
                ],
            },
        ],
        "retain_matching_columns": True,
        "retain_intermediate_calculation_columns": True,
    }

    def gamma_lev_from_distance(dist):
        # which gamma value will I get for a given levenshtein distance?
        if dist == 0:
            return 4
        elif dist == 1:
            return 3
        elif dist == 2:
            return 2
        elif dist == 3:
            return 1
        elif dist > 3:
            return 0
        raise ValueError(f"Invalid distance supplied ({dist})")

    df = pd.DataFrame(data)
    df = helper.convert_frame(df)

    linker = helper.Linker(df, settings, **helper.extra_linker_args())
    df_e = linker.predict().as_pandas_dataframe()

    for id_r, lev_dist in id_distance_from_1.items():
        expected_gamma_lev = gamma_lev_from_distance(lev_dist)
        row = dict(df_e.query(f"id_l == 1 and id_r == {id_r}").iloc[0])
        assert row["gamma_name"] == expected_gamma_lev
