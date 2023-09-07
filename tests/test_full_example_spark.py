import os

import pandas as pd
import pyspark.sql.functions as f
import pytest
from pyspark.sql.types import StringType, StructField, StructType

import splink.spark.comparison_level_library as cll
import splink.spark.comparison_library as cl
from splink.spark.linker import SparkLinker

from .basic_settings import get_settings_dict, name_comparison
from .decorator import mark_with_dialects_including
from .linker_utils import (
    _test_write_functionality,
    register_roc_data,
)


def test_full_example_spark(df_spark, tmp_path):
    # Annoyingly, this needs an independent linker as csv doesn't
    # accept arrays as inputs, which we are adding to df_spark below
    linker = SparkLinker(df_spark, get_settings_dict())

    # Test that writing to files works as expected
    def spark_csv_read(x):
        return linker.spark.read.csv(x, header=True).toPandas()

    _test_write_functionality(linker, spark_csv_read)

    # Convert a column to an array to enable testing intersection
    df_spark = df_spark.withColumn("email", f.array("email"))
    settings_dict = get_settings_dict()

    # Only needed because the value can be overwritten by other tests
    settings_dict["comparisons"][1] = cl.exact_match("surname")
    settings_dict["comparisons"].append(name_comparison(cll, "surname"))

    settings = {
        "probability_two_random_records_match": 0.01,
        "link_type": "dedupe_only",
        "blocking_rules_to_generate_predictions": [
            {"blocking_rule": "l.surname = r.surname", "salting_partitions": 3},
        ],
        "comparisons": [
            cl.jaro_winkler_at_thresholds("first_name", 0.9),
            cl.jaro_at_thresholds("surname", 0.9),
            cl.damerau_levenshtein_at_thresholds("dob", 2),
            {
                "comparison_levels": [
                    cll.array_intersect_level("email"),
                    cll.else_level(),
                ]
            },
            cl.jaccard_at_thresholds("city", [0.9]),
        ],
        "retain_matching_columns": True,
        "retain_intermediate_calculation_columns": True,
        "additional_columns_to_retain": ["group"],
        "em_convergence": 0.01,
        "max_iterations": 2,
    }

    linker = SparkLinker(
        df_spark,
        settings,
        break_lineage_method="checkpoint",
        num_partitions_on_repartition=2,
    )

    linker.profile_columns(
        ["first_name", "surname", "first_name || surname", "concat(city, first_name)"]
    )
    linker.compute_tf_table("city")
    linker.compute_tf_table("first_name")

    linker.estimate_probability_two_random_records_match(
        ["l.email = r.email"], recall=0.3
    )
    linker.estimate_u_using_random_sampling(max_pairs=1e5, seed=1)

    blocking_rule = "l.first_name = r.first_name and l.surname = r.surname"
    linker.estimate_parameters_using_expectation_maximisation(blocking_rule)

    blocking_rule = "l.dob = r.dob"
    linker.estimate_parameters_using_expectation_maximisation(blocking_rule)

    df_predict = linker.predict()

    linker.comparison_viewer_dashboard(
        df_predict, os.path.join(tmp_path, "test_scv_spark.html"), True, 2
    )

    df_clusters = linker.cluster_pairwise_predictions_at_threshold(df_predict, 0.2)

    linker.cluster_studio_dashboard(
        df_predict,
        df_clusters,
        cluster_ids=[0, 4],
        cluster_names=["cluster_0", "cluster_4"],
        out_path=os.path.join(tmp_path, "test_cluster_studio.html"),
    )

    linker.unlinkables_chart(source_dataset="Testing")
    # Test that writing to files works as expected
    # spark_csv_read = lambda x: linker.spark.read.csv(x, header=True).toPandas()
    # _test_write_functionality(linker, spark_csv_read)

    # Check spark tables are being registered correctly
    StructType(
        [
            StructField("firstname", StringType(), True),
            StructField("lastname", StringType(), True),
        ]
    )
    register_roc_data(linker)
    linker.roc_chart_from_labels_table("labels")
    linker.accuracy_chart_from_labels_table("labels")
    linker.confusion_matrix_from_labels_table("labels")

    record = {
        "unique_id": 1,
        "first_name": "John",
        "surname": "Smith",
        "dob": "1971-05-24",
        "city": "London",
        "email": ["john@smith.net"],
        "group": 10000,
    }

    linker.find_matches_to_new_records(
        [record], blocking_rules=[], match_weight_threshold=-10000
    )

    # Test differing inputs are accepted
    settings["link_type"] = "link_only"

    linker = SparkLinker(
        [df_spark, df_spark.toPandas()],
        settings,
        break_lineage_method="checkpoint",
        num_partitions_on_repartition=2,
    )

    # Test saving and loading
    path = os.path.join(tmp_path, "model.json")
    linker.save_model_to_json(path)

    linker_2 = SparkLinker(df_spark)
    linker_2.load_model(path)
    linker_2.load_settings(path)
    linker_2.load_settings_from_json(path)
    SparkLinker(df_spark, settings_dict=path)


def test_link_only(df_spark):
    settings = get_settings_dict()
    settings["link_type"] = "link_only"
    settings["source_dataset_column_name"] = "source_dataset"

    df_spark_a = df_spark.withColumn("source_dataset", f.lit("my_left_ds"))
    df_spark_b = df_spark.withColumn("source_dataset", f.lit("my_right_ds"))

    linker = SparkLinker(
        [df_spark_a, df_spark_b],
        settings,
        break_lineage_method="checkpoint",
        num_partitions_on_repartition=2,
    )
    df_predict = linker.predict().as_pandas_dataframe()

    assert len(df_predict) == 7257
    assert set(df_predict.source_dataset_l.values) == {"my_left_ds"}
    assert set(df_predict.source_dataset_r.values) == {"my_right_ds"}


@pytest.mark.parametrize(
    ("df"),
    [
        pytest.param(
            pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv"),
            id="Spark load from pandas df",
        )
    ],
)
@mark_with_dialects_including("spark")
def test_spark_load_from_file(df, spark):
    settings = get_settings_dict()

    linker = SparkLinker(
        df,
        settings,
        spark=spark,
    )

    assert len(linker.predict().as_pandas_dataframe()) == 3167
