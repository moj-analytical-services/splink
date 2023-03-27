import os

import pandas as pd
import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.parquet as pq
import pytest
from basic_settings import get_settings_dict
from linker_utils import _test_table_registration, register_roc_data

from splink.duckdb.duckdb_comparison_library import (
    exact_match,
    jaccard_at_thresholds,
    jaro_winkler_at_thresholds,
)
from splink.duckdb.duckdb_linker import DuckDBLinker


def test_full_example_duckdb(tmp_path):
    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
    df = df.rename(columns={"surname": "SUR name"})
    settings_dict = get_settings_dict()

    # Overwrite the surname comparison to include duck-db specific syntax
    settings_dict["comparisons"][0] = jaro_winkler_at_thresholds("first_name")
    settings_dict["comparisons"][1] = jaccard_at_thresholds("SUR name")
    settings_dict["blocking_rules_to_generate_predictions"] = [
        'l."SUR name" = r."SUR name"',
    ]

    linker = DuckDBLinker(
        df,
        connection=os.path.join(tmp_path, "duckdb.db"),
        output_schema="splink_in_duckdb",
    )
    linker.load_settings(settings_dict)

    linker.count_num_comparisons_from_blocking_rule(
        'l.first_name = r.first_name and l."SUR name" = r."SUR name"'
    )

    linker.profile_columns(
        [
            "first_name",
            '"SUR name"',
            'first_name || "SUR name"',
            "concat(city, first_name)",
        ]
    )
    linker.missingness_chart()
    linker.compute_tf_table("city")
    linker.compute_tf_table("first_name")

    linker.estimate_u_using_random_sampling(max_pairs=1e6)
    linker.estimate_probability_two_random_records_match(
        ["l.email = r.email"], recall=0.3
    )
    # try missingness chart again now that concat_with_tf is precomputed
    linker.missingness_chart()

    blocking_rule = 'l.first_name = r.first_name and l."SUR name" = r."SUR name"'
    linker.estimate_parameters_using_expectation_maximisation(blocking_rule)

    blocking_rule = "l.dob = r.dob"
    linker.estimate_parameters_using_expectation_maximisation(blocking_rule)

    df_predict = linker.predict()

    linker.comparison_viewer_dashboard(
        df_predict, os.path.join(tmp_path, "test_scv_duckdb.html"), True, 2
    )

    df_e = df_predict.as_pandas_dataframe(limit=5)
    records = df_e.to_dict(orient="records")
    linker.waterfall_chart(records)

    register_roc_data(linker)
    linker.roc_chart_from_labels_table("labels")

    df_clusters = linker.cluster_pairwise_predictions_at_threshold(df_predict, 0.1)

    linker.cluster_studio_dashboard(
        df_predict,
        df_clusters,
        sampling_method="by_cluster_size",
        out_path=os.path.join(tmp_path, "test_cluster_studio.html"),
    )

    linker.unlinkables_chart(source_dataset="Testing")

    _test_table_registration(linker)

    record = {
        "unique_id": 1,
        "first_name": "John",
        "SUR name": "Smith",
        "dob": "1971-05-24",
        "city": "London",
        "email": "john@smith.net",
        "group": 10000,
    }

    linker.find_matches_to_new_records(
        [record], blocking_rules=[], match_weight_threshold=-10000
    )

    # Test saving and loading
    path = os.path.join(tmp_path, "model.json")
    linker.save_settings_to_json(path)

    linker_2 = DuckDBLinker(df, connection=":memory:")
    linker_2.load_settings(path)
    linker_2.load_settings_from_json(path)
    DuckDBLinker(df, settings_dict=path)


@pytest.mark.parametrize(
    ("df"),
    [
        pytest.param(
            pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv"),
            id="DuckDB link from pandas df",
        ),
        pytest.param(
            "./tests/datasets/fake_1000_from_splink_demos.csv",
            id="DuckDB load from file",
        ),
        pytest.param(
            pa.Table.from_pandas(
                pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
            ),
            id="DuckDB link - convert pandas to pyarrow df",
        ),
        pytest.param(
            pa_csv.read_csv(
                "./tests/datasets/fake_1000_from_splink_demos.csv",
                convert_options=pa_csv.ConvertOptions(
                    strings_can_be_null=True, null_values=["", "", "NULL"]
                ),
            ),
            id="DuckDB link - read directly from filepath with pyarrow",
        ),
    ],
)
def test_duckdb_load_from_file(df):
    settings = get_settings_dict()

    linker = DuckDBLinker(
        df,
        settings,
    )

    assert len(linker.predict().as_pandas_dataframe()) == 3167

    settings["link_type"] = "link_only"

    linker = DuckDBLinker(
        [df, df],
        settings,
        input_table_aliases=["testing1", "testing2"],
    )

    assert len(linker.predict().as_pandas_dataframe()) == 7257


def test_duckdb_arrow_array():
    # Checking array fixes problem identified here:
    # https://github.com/moj-analytical-services/splink/issues/680

    f = "./tests/datasets/test_array.parquet"
    array_data = pq.read_table(f)

    # data is:
    # data_list = [
    # {"uid": 1, "a": ['robin', 'john'], "b": 1},
    # {"uid": 1, "a": ['robin', 'john'], "b": 1},
    # {"uid": 1, "a": ['james', 'karen'], "b": 1},
    # {"uid": 1, "a": ['james', 'john'], "b": 1},
    #     ]

    linker = DuckDBLinker(
        array_data,
        {
            "link_type": "dedupe_only",
            "unique_id_column_name": "uid",
            "comparisons": [exact_match("b")],
            "blocking_rules_to_generate_predictions": ["l.a[1] = r.a[1]"],
        },
    )
    df = linker.deterministic_link().as_pandas_dataframe()
    assert len(df) == 2


def test_cast_error():
    from duckdb import InvalidInputException

    forenames = [None, "jack", None] * 1000
    data = {"id": range(0, len(forenames)), "forename": forenames}
    df = pd.DataFrame(data)

    with pytest.raises(InvalidInputException):
        DuckDBLinker(df)

    # convert to pyarrow table
    df = pa.Table.from_pandas(df)
    DuckDBLinker(df)
