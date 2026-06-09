import os

import duckdb
import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq
import pytest

import splink.internals.comparison_level_library as cll
import splink.internals.comparison_library as cl
from splink import ColumnExpression, DuckDBAPI, Linker, SettingsCreator, block_on
from splink.blocking_analysis import count_comparisons_from_blocking_rules
from splink.exploratory import completeness_chart, profile_columns

from .basic_settings import get_settings_dict, name_comparison
from .decorator import mark_with_dialects_including

simple_settings = {
    "link_type": "dedupe_only",
}


@mark_with_dialects_including("duckdb")
def test_full_example_duckdb(tmp_path, fake_1000):
    df = fake_1000.rename_columns({"surname": "SUR name"})
    settings_dict = get_settings_dict()

    # Overwrite the surname comparison to include duck-db specific syntax
    settings_dict["comparisons"].append(name_comparison(cll, "SUR name"))
    settings_dict["comparisons"][1] = cl.JaccardAtThresholds("SUR name")

    settings_dict["blocking_rules_to_generate_predictions"] = [
        'l."SUR name" = r."SUR name"',
    ]

    db_api = DuckDBAPI(connection=os.path.join(tmp_path, "duckdb.db"))
    df_sdf = db_api.register(df)

    count_comparisons_from_blocking_rules(
        df_sdf,
        blocking_rules='l.first_name = r.first_name and l."SUR name" = r."SUR name"',  # noqa: E501
        link_type="dedupe_only",
        unique_id_column_name="unique_id",
    )

    linker = Linker(
        df_sdf,
        settings=settings_dict,
        # output_schema="splink_in_duckdb",
    )

    profile_columns(
        df_sdf,
        [
            "first_name",
            '"SUR name"',
            'first_name || "SUR name"',
            "concat(city, first_name)",
        ],
    )
    completeness_chart(df_sdf)

    linker.table_management.compute_tf_table("city")
    linker.table_management.compute_tf_table("first_name")

    linker.training.estimate_u_using_random_sampling(max_pairs=1e6, seed=1)
    linker.training.estimate_probability_two_random_records_match(
        ["l.email = r.email"], recall=0.3
    )

    blocking_rule = 'l.first_name = r.first_name and l."SUR name" = r."SUR name"'
    linker.training.estimate_parameters_using_expectation_maximisation(blocking_rule)

    blocking_rule = "l.dob = r.dob"
    linker.training.estimate_parameters_using_expectation_maximisation(blocking_rule)

    df_predict = linker.inference.predict()

    linker.visualisations.comparison_viewer_dashboard(
        df_predict, os.path.join(tmp_path, "test_scv_duckdb.html"), True, 2
    )

    records = df_predict.as_record_dict(limit=5)
    linker.visualisations.waterfall_chart(records)

    labels_sdf = df_sdf.query_sql(
        """
        WITH first_10 AS (
            SELECT * FROM {this} LIMIT 10
        )
        SELECT
            l.unique_id AS unique_id_l,
            r.unique_id AS unique_id_r,
            CAST(l.cluster = r.cluster AS float) AS clerical_match_score
        FROM
            first_10 l
        CROSS JOIN
            first_10 r
        WHERE
            unique_id_l < unique_id_r
        """
    )
    linker.evaluation.accuracy_analysis_from_labels_table(labels_sdf.physical_name)

    df_clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(
        df_predict, 0.1
    )

    linker.visualisations.cluster_studio_dashboard(
        df_predict,
        df_clusters,
        sampling_method="by_cluster_size",
        out_path=os.path.join(tmp_path, "test_cluster_studio.html"),
    )

    linker.evaluation.unlinkables_chart(name_of_data_in_title="Testing")

    record = {
        "unique_id": 1,
        "first_name": "John",
        "SUR name": "Smith",
        "dob": "1971-05-24",
        "city": "London",
        "email": "john@smith.net",
        "cluster": 10000,
    }

    linker.inference.find_matches_to_new_records(
        [record], blocking_rules=[], match_weight_threshold=-10000
    )

    # Test saving and loading
    path = os.path.join(tmp_path, "model.json")
    linker.misc.save_model_to_json(path)

    db_api = DuckDBAPI()
    df_sdf2 = db_api.register(df)
    Linker(df_sdf2, settings=simple_settings)

    Linker(df_sdf2, settings=path)


# Create some dummy dataframes for the link only test
df = pv.read_csv(
    "./tests/datasets/fake_1000_from_splink_demos.csv",
    convert_options=pv.ConvertOptions(strings_can_be_null=True),
)
df_l = df.append_column("source_dataset", pa.array(1000 * ["my_left_ds"]))
df_r = df.append_column("source_dataset", pa.array(1000 * ["my_right_ds"]))
df_final = pa.concat_tables([df_l, df_r])


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
    if isinstance(input, list):
        input_sdf = [db_api.register(inp) for inp in input]
    else:
        input_sdf = db_api.register(input)

    linker = Linker(input_sdf, settings)
    sdf_predict = linker.inference.predict()
    predict_dict = sdf_predict.as_dict()

    assert len(sdf_predict.as_record_dict()) == 7257
    assert set(predict_dict["source_dataset_l"]) == source_l
    assert set(predict_dict["source_dataset_r"]) == source_r


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
    array_data_sdf = db_api.register(array_data)
    linker = Linker(
        array_data_sdf,
        {
            "link_type": "dedupe_only",
            "unique_id_column_name": "uid",
            "comparisons": [cl.ExactMatch("b")],
            "blocking_rules_to_generate_predictions": ["l.a[1] = r.a[1]"],
        },
    )
    df = linker.inference.deterministic_link().as_record_dict()
    assert len(df) == 2


@mark_with_dialects_including("duckdb")
def test_small_example_duckdb(fake_1000):
    df = fake_1000.append_column(
        "full_name",
        pa.array(
            [
                f"{fn}_{sn}"
                for fn, sn in zip(fake_1000["first_name"], fake_1000["surname"])
            ]
        ),
    )

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
            cl.DamerauLevenshteinAtThresholds(
                ColumnExpression("dob").cast_to_string(), 2
            ).configure(term_frequency_adjustments=True),
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
    df_sdf = db_api.register(df)
    linker = Linker(df_sdf, settings_dict)

    linker.training.estimate_u_using_random_sampling(max_pairs=1e6)
    blocking_rule = "l.full_name = r.full_name"
    linker.training.estimate_parameters_using_expectation_maximisation(blocking_rule)

    blocking_rule = "l.dob = r.dob"
    linker.training.estimate_parameters_using_expectation_maximisation(blocking_rule)

    linker.inference.predict()


@mark_with_dialects_including("duckdb")
def test_duckdb_input_is_duckdbpyrelation(fake_1000):
    df1 = duckdb.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    settings = SettingsCreator(
        link_type="link_and_dedupe",
        comparisons=[cl.ExactMatch("first_name")],
        blocking_rules_to_generate_predictions=[block_on("first_name", "surname")],
    )
    db_api = DuckDBAPI(connection=":default:")
    df1_sdf = db_api.register(df1)
    df2_sdf = db_api.register(fake_1000)
    linker = Linker([df1_sdf, df2_sdf], settings)
    linker.inference.predict()
