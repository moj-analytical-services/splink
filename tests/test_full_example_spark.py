import os

from splink.spark.spark_linker import SparkLinker
import splink.spark.spark_comparison_library as cl
from splink.spark.spark_comparison_level_library import _mutable_params

from basic_settings import get_settings_dict


def test_full_example_spark(df_spark, tmp_path):
    settings_dict = get_settings_dict()

    _mutable_params["dialect"] = "duckdb"
    settings_dict["comparisons"][1] = cl.exact_match("surname")

    linker = SparkLinker(df_spark, settings_dict, break_lineage_method="checkpoint")

    linker.profile_columns(
        ["first_name", "surname", "first_name || surname", "concat(city, first_name)"]
    )
    linker.compute_tf_table("city")
    linker.compute_tf_table("first_name")

    linker.estimate_u_using_random_sampling(target_rows=1e6)

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
        [0, 4],
        os.path.join(tmp_path, "test_cluster_studio.html"),
    )
