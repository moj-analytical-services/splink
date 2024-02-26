import os

import pandas as pd
import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.parquet as pq
import pytest

import splink.comparison_level_library as cll
import splink.comparison_library as cl
from splink.database_api import DuckDBAPI
from splink.linker import Linker
from splink.profile_data import profile_columns

from .basic_settings import get_settings_dict, name_comparison
from .decorator import mark_with_dialects_including
from .linker_utils import (
    _test_table_registration,
    _test_write_functionality,
    register_roc_data,
)

simple_settings = {
    "link_type": "dedupe_only",
}


@mark_with_dialects_including("duckdb")
def test_full_example_duckdb(tmp_path):
    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
    df = df.rename(columns={"surname": "SUR name"})
    settings_dict = get_settings_dict()

    # Overwrite the surname comparison to include duck-db specific syntax
    settings_dict["comparisons"].append(name_comparison(cll, "SUR name"))
    settings_dict["comparisons"][1] = cl.JaccardAtThresholds("SUR name")

    settings_dict["blocking_rules_to_generate_predictions"] = [
        'l."SUR name" = r."SUR name"',
    ]

    db_api = DuckDBAPI(connection=os.path.join(tmp_path, "duckdb.db"))
    linker = Linker(
        df,
        settings=settings_dict,
        database_api=db_api,
        # output_schema="splink_in_duckdb",
    )

    linker.count_num_comparisons_from_blocking_rule(
        'l.first_name = r.first_name and l."SUR name" = r."SUR name"'
    )

    profile_columns(
        df,
        db_api,
        [
            "first_name",
            '"SUR name"',
            'first_name || "SUR name"',
            "concat(city, first_name)",
        ],
    )
    linker.missingness_chart()
    linker.compute_tf_table("city")
    linker.compute_tf_table("first_name")

    linker.estimate_u_using_random_sampling(max_pairs=1e6, seed=1)
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
    linker.accuracy_chart_from_labels_table("labels")
    linker.confusion_matrix_from_labels_table("labels")

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
        "cluster": 10000,
    }

    linker.find_matches_to_new_records(
        [record], blocking_rules=[], match_weight_threshold=-10000
    )

    # Test saving and loading
    path = os.path.join(tmp_path, "model.json")
    linker.save_model_to_json(path)

    db_api = DuckDBAPI()
    linker_2 = Linker(df, settings=simple_settings, database_api=db_api)

    linker_2 = Linker(df, database_api=db_api, settings=path)

    # Test that writing to files works as expected
    _test_write_functionality(linker_2, pd.read_csv)


# Create some dummy dataframes for the link only test
df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
df_l = df.copy()
df_r = df.copy()
df_l["source_dataset"] = "my_left_ds"
df_r["source_dataset"] = "my_right_ds"
df_final = pd.concat([df_l, df_r])


# Tests link only jobs under different inputs:
# * A single dataframe with a `source_dataset` column
# * Two input dataframes with no specified `source_dataset` column
# * Two input dataframes with a specified `source_dataset` column
@pytest.mark.parametrize(
    ("input", "source_l", "source_r"),
    [
        pytest.param(
            [df, df],  # no source_dataset col
            {"__splink__input_table_0"},
            {"__splink__input_table_1"},
            id="No source dataset column",
        ),
        pytest.param(
            df_final,  # source_dataset col
            {"my_left_ds"},
            {"my_right_ds"},
            id="Source dataset column in a single df",
        ),
        pytest.param(
            [df_l, df_r],  # source_dataset col
            {"my_left_ds"},
            {"my_right_ds"},
            id="Source dataset column in two dfs",
        ),
    ],
)
@mark_with_dialects_including("duckdb")
def test_link_only(input, source_l, source_r):
    settings = get_settings_dict()
    settings["link_type"] = "link_only"
    settings["source_dataset_column_name"] = "source_dataset"

    db_api = DuckDBAPI()
    linker = Linker(input, settings, database_api=db_api)
    df_predict = linker.predict().as_pandas_dataframe()

    assert len(df_predict) == 7257
    assert set(df_predict.source_dataset_l.values) == source_l
    assert set(df_predict.source_dataset_r.values) == source_r


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
@mark_with_dialects_including("duckdb")
def test_duckdb_load_from_file(df):
    settings = get_settings_dict()

    db_api = DuckDBAPI()
    linker = Linker(
        df,
        settings,
        database_api=db_api,
    )

    assert len(linker.predict().as_pandas_dataframe()) == 3167

    settings["link_type"] = "link_only"

    db_api = DuckDBAPI()
    linker = Linker(
        [df, df],
        settings,
        database_api=db_api,
        input_table_aliases=["testing1", "testing2"],
    )

    assert len(linker.predict().as_pandas_dataframe()) == 7257


@mark_with_dialects_including("duckdb")
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

    db_api = DuckDBAPI()
    linker = Linker(
        array_data,
        {
            "link_type": "dedupe_only",
            "unique_id_column_name": "uid",
            "comparisons": [cl.ExactMatch("b")],
            "blocking_rules_to_generate_predictions": ["l.a[1] = r.a[1]"],
        },
        database_api=db_api,
    )
    df = linker.deterministic_link().as_pandas_dataframe()
    assert len(df) == 2


@mark_with_dialects_including("duckdb")
def test_cast_error():
    from duckdb import InvalidInputException

    forenames = [None, "jack", None] * 1000
    data = {"id": range(0, len(forenames)), "forename": forenames}
    df = pd.DataFrame(data)

    db_api = DuckDBAPI()
    with pytest.raises(InvalidInputException):
        Linker(df, settings=simple_settings, database_api=db_api)

    # convert to pyarrow table
    df = pa.Table.from_pandas(df)
    Linker(df, settings=simple_settings, database_api=db_api)


@mark_with_dialects_including("duckdb")
def test_small_example_duckdb(tmp_path):
    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
    df["full_name"] = df["first_name"] + df["surname"]

    settings_dict = {
        "link_type": "dedupe_only",
        "blocking_rules_to_generate_predictions": [
            "l.surname = r.surname",
            "l.city = r.city",
        ],
        "comparisons": [
            {
                "output_column_name": "name",
                "comparison_levels": [
                    cll.NullLevel("full_name", valid_string_pattern=".*"),
                    cll.ExactMatchLevel("full_name", term_frequency_adjustments=True),
                    cll.ColumnsReversedLevel("first_name", "surname").configure(
                        tf_adjustment_column="full_name"
                    ),
                    cll.ExactMatchLevel("first_name", term_frequency_adjustments=True),
                    cll.ElseLevel(),
                ],
            },
            cl.DamerauLevenshteinAtThresholds("dob", 2).configure(
                term_frequency_adjustments=True
            ),
            cl.JaroAtThresholds("email", 0.9).configure(
                term_frequency_adjustments=True
            ),
            cl.JaroWinklerAtThresholds("city", 0.9).configure(
                term_frequency_adjustments=True
            ),
        ],
        "retain_matching_columns": True,
        "retain_intermediate_calculation_columns": True,
    }

    db_api = DuckDBAPI()
    linker = Linker(df, settings_dict, database_api=db_api)

    linker.estimate_u_using_random_sampling(max_pairs=1e6)
    blocking_rule = "l.full_name = r.full_name"
    linker.estimate_parameters_using_expectation_maximisation(blocking_rule)

    blocking_rule = "l.dob = r.dob"
    linker.estimate_parameters_using_expectation_maximisation(blocking_rule)

    linker.predict()
