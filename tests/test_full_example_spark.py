import os

from splink.spark.spark_linker import SparkLinker
import splink.spark.spark_comparison_library as cl
from splink.spark.spark_comparison_level_library import (
    _mutable_params,
    array_intersect_level,
    else_level,
)

from basic_settings import get_settings_dict

from pyspark.sql.functions import array


def test_full_example_spark(df_spark, tmp_path):

    # Convert a column to an array to enable testing intersection
    df_spark = df_spark.withColumn("email", array("email"))

    settings_dict = get_settings_dict()

    # Only needed because the value can be overwritten by other tests
    _mutable_params["dialect"] = "spark"
    settings_dict["comparisons"][1] = cl.exact_match("surname")

    settings = {
        "proportion_of_matches": 0.01,
        "link_type": "dedupe_only",
        "blocking_rules_to_generate_predictions": [
            "l.surname = r.surname",
        ],
        "comparisons": [
            cl.levenshtein_at_thresholds("first_name", 2),
            cl.exact_match("surname"),
            cl.exact_match("dob"),
            {
                "comparison_levels": [
                    array_intersect_level("email"),
                    else_level(),
                ]
            },
            cl.exact_match("city"),
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

    linker.estimate_u_using_random_sampling(target_rows=1e5)

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

    linker.unlinkables_chart(source_dataset="Testing")
