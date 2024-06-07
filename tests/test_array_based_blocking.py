import random

import pandas as pd

import splink.internals.comparison_library as cl
from tests.decorator import mark_with_dialects_including


@mark_with_dialects_including("duckdb", "spark", pass_dialect=True)
def test_simple_example_link_only(test_helpers, dialect):
    data_l = pd.DataFrame.from_dict(
        [
            {"unique_id": 1, "gender": "m", "postcode": ["2612", "2000"]},
            {"unique_id": 2, "gender": "m", "postcode": ["2612", "2617"]},
            {"unique_id": 3, "gender": "f", "postcode": ["2617"]},
        ]
    )
    data_r = pd.DataFrame.from_dict(
        [
            {"unique_id": 4, "gender": "m", "postcode": ["2617", "2600"]},
            {"unique_id": 5, "gender": "f", "postcode": ["2000"]},
            {"unique_id": 6, "gender": "m", "postcode": ["2617", "2612", "2000"]},
        ]
    )
    helper = test_helpers[dialect]
    settings = {
        "link_type": "link_only",
        "blocking_rules_to_generate_predictions": [
            {
                "blocking_rule": "l.gender = r.gender and l.postcode = r.postcode",
                "arrays_to_explode": ["postcode"],
            },
            "l.gender = r.gender",
        ],
        "comparisons": [cl.ArrayIntersectAtSizes("postcode", [1])],
    }
    ## the pairs returned by the first blocking rule are (1,6),(2,4),(2,6)
    ## the additional pairs returned by the second blocking rule are (1,4),(3,5)
    linker = helper.Linker([data_l, data_r], settings, **helper.extra_linker_args())
    linker.debug_mode = False
    returned_triples = linker.inference.predict().as_pandas_dataframe()[
        ["unique_id_l", "unique_id_r", "match_key"]
    ]
    returned_triples = {
        (unique_id_l, unique_id_r, match_key)
        for unique_id_l, unique_id_r, match_key in zip(
            returned_triples.unique_id_l,
            returned_triples.unique_id_r,
            returned_triples.match_key,
        )
    }
    expected_triples = {(1, 6, "0"), (2, 4, "0"), (2, 6, "0"), (1, 4, "1"), (3, 5, "1")}
    assert expected_triples == returned_triples


def generate_array_based_datasets_helper(
    n_rows=1000, n_array_based_columns=3, n_distinct_values=1000, array_size=3, seed=1
):
    random.seed(seed)
    datasets = []
    for _k in range(2):
        results_dict = {}
        results_dict["cluster"] = list(range(n_rows))
        for i in range(n_array_based_columns):
            col = []
            for j in range(n_rows):
                col.append(random.sample(range(n_distinct_values), array_size))
                if random.random() < 0.8 or i == n_array_based_columns - 1:
                    col[-1].append(j)
                    random.shuffle(col[-1])
            results_dict[f"array_column_{i}"] = col
        datasets.append(pd.DataFrame.from_dict(results_dict))
    return datasets


@mark_with_dialects_including("duckdb", "spark", pass_dialect=True)
def test_array_based_blocking_with_random_data_dedupe(test_helpers, dialect):
    helper = test_helpers[dialect]
    input_data_l, input_data_r = generate_array_based_datasets_helper()
    input_data_l = input_data_l.assign(
        unique_id=[str(cluster_id) + "-0" for cluster_id in input_data_l.cluster]
    )
    input_data_r = input_data_r.assign(
        unique_id=[str(cluster_id) + "-1" for cluster_id in input_data_r.cluster]
    )
    input_data = pd.concat([input_data_l, input_data_r])
    blocking_rules = [
        {
            "blocking_rule": """l.array_column_0 = r.array_column_0
                and l.array_column_1 = r.array_column_1""",
            "arrays_to_explode": ["array_column_0", "array_column_1"],
        },
        {
            "blocking_rule": """l.array_column_0 = r.array_column_0
                and l.array_column_1 = r.array_column_1
                and l.array_column_2 = r.array_column_2""",
            "arrays_to_explode": ["array_column_0", "array_column_1"],
        },
        {
            "blocking_rule": "l.array_column_2 = r.array_column_2",
            "arrays_to_explode": ["array_column_2"],
        },
    ]
    settings = {
        "link_type": "dedupe_only",
        "blocking_rules_to_generate_predictions": blocking_rules,
        "unique_id_column_name": "unique_id",
        "additional_columns_to_retain": ["cluster"],
        "comparisons": [cl.ArrayIntersectAtSizes("array_column_1", [1])],
    }
    linker = helper.Linker(input_data, settings, **helper.extra_linker_args())
    linker.debug_mode = False
    df_predict = linker.inference.predict().as_pandas_dataframe()
    ## check that there are no duplicates in the output
    assert (
        df_predict.drop_duplicates(["unique_id_l", "unique_id_r"]).shape[0]
        == df_predict.shape[0]
    )

    ## check that the output contains no links with match_key=1,
    ## since all pairs returned by the second rule should also be
    ## returned by the first rule and so should be filtered out
    assert df_predict[df_predict.match_key == 1].shape[0] == 0

    ## check that all 1000 true matches are in the output
    ## (this is guaranteed by how the data was generated)
    assert sum(df_predict.cluster_l == df_predict.cluster_r) == 1000


@mark_with_dialects_including("duckdb", "spark", pass_dialect=True)
def test_array_based_blocking_with_random_data_link_only(test_helpers, dialect):
    helper = test_helpers[dialect]
    input_data_l, input_data_r = generate_array_based_datasets_helper()
    blocking_rules = [
        {
            "blocking_rule": """l.array_column_0 = r.array_column_0
                and l.array_column_1 = r.array_column_1""",
            "arrays_to_explode": ["array_column_0", "array_column_1"],
        },
        {
            "blocking_rule": """l.array_column_0 = r.array_column_0
                and l.array_column_1 = r.array_column_1
                and l.array_column_2=r.array_column_2""",
            "arrays_to_explode": ["array_column_0", "array_column_1", "array_column_2"],
        },
        {
            "blocking_rule": "l.array_column_2 = r.array_column_2",
            "arrays_to_explode": ["array_column_2"],
        },
    ]
    settings = {
        "link_type": "link_only",
        "blocking_rules_to_generate_predictions": blocking_rules,
        "unique_id_column_name": "cluster",
        "additional_columns_to_retain": ["cluster"],
        "comparisons": [cl.ArrayIntersectAtSizes("array_column_1", [1])],
    }
    linker = helper.Linker(
        [input_data_l, input_data_r], settings, **helper.extra_linker_args()
    )
    linker.debug_mode = False
    df_predict = linker.inference.predict().as_pandas_dataframe()

    ## check that we get no within-dataset links
    within_dataset_links = df_predict[
        df_predict.source_dataset_l == df_predict.source_dataset_r
    ].shape[0]
    assert within_dataset_links == 0

    ## check that no pair of ids appears twice in the output
    assert (
        df_predict.drop_duplicates(["cluster_l", "cluster_r"]).shape[0]
        == df_predict.shape[0]
    )

    ## check that the second blocking rule returns no matches,
    ## since every pair matching the second rule will also match the first,
    ## and so should be filtered out
    assert df_predict[df_predict.match_key == 1].shape[0] == 0

    ## check that all 1000 true matches are returned
    assert sum(df_predict.cluster_l == df_predict.cluster_r) == 1000


@mark_with_dialects_including("duckdb", pass_dialect=True)
def test_link_only_unique_id_ambiguity(test_helpers, dialect):
    helper = test_helpers[dialect]
    data_1 = [
        {
            "unique_id": 1,
            "first_name": "John",
            "surname": "Doe",
            "postcode": ["A", "B"],
        },
        {"unique_id": 3, "first_name": "John", "surname": "Doe", "postcode": ["B"]},
    ]

    data_2 = [
        {"unique_id": 3, "first_name": "John", "surname": "Smith", "postcode": ["A"]},
    ]

    data_3 = [
        {"unique_id": 3, "first_name": "John", "surname": "Smith", "postcode": ["A"]},
        {"unique_id": 4, "first_name": "John", "surname": "Doe", "postcode": ["C"]},
    ]

    df_1 = pd.DataFrame(data_1)
    df_2 = pd.DataFrame(data_2)
    df_3 = pd.DataFrame(data_3)

    settings = {
        "link_type": "link_only",
        "blocking_rules_to_generate_predictions": [
            {
                "blocking_rule": """l.postcode = r.postcode
                                    and l.first_name = r.first_name""",
                "arrays_to_explode": ["postcode"],
            },
            "l.surname = r.surname",
        ],
        "comparisons": [
            cl.ExactMatch("first_name"),
            cl.ExactMatch("surname"),
            cl.ExactMatch("postcode"),
        ],
        "retain_intermediate_calculation_columns": True,
    }

    linker = helper.Linker(
        [df_1, df_2, df_3],
        settings,
        input_table_aliases=["a_", "b_", "c_"],
        **helper.extra_linker_args(),
    )
    returned_triples = linker.inference.predict().as_pandas_dataframe()[
        [
            "source_dataset_l",
            "unique_id_l",
            "source_dataset_r",
            "unique_id_r",
            "match_key",
        ]
    ]

    actual_triples = {
        tuple(t) for t in returned_triples.to_dict(orient="split")["data"]
    }
    assert len(returned_triples) == 5

    rule1_tuples = {
        ("a_", 1, "b_", 3, "0"),
        ("a_", 1, "c_", 3, "0"),
        ("b_", 3, "c_", 3, "0"),
    }
    rule2_tuples = {("a_", 1, "c_", 4, "1"), ("a_", 3, "c_", 4, "1")}

    all_tuples = rule1_tuples.union(rule2_tuples)
    assert actual_triples == all_tuples
