import splink.spark.spark_comparison_library as cl


def test_distance_function_comparison():

    settings = {
        "link_type": "dedupe_only",
        "comparisons": [
            cl.distance_function_at_thresholds("forename", "hamming", [1, 2])
        ],
    }
