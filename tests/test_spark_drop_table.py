import splink.comparison_library as cl
from splink import Linker, SparkAPI, block_on

from .decorator import mark_with_dialects_including


@mark_with_dialects_including("spark")
def test_spark_drops_tables(spark, df_spark, tmp_path, spark_api):
    settings = {
        "probability_two_random_records_match": 0.01,
        "link_type": "dedupe_only",
        "blocking_rules_to_generate_predictions": [
            block_on("surname"),
        ],
        "comparisons": [
            cl.ExactMatch("surname"),
        ],
        "retain_matching_columns": True,
        "retain_intermediate_calculation_columns": True,
        "additional_columns_to_retain": ["cluster"],
    }

    linker = Linker(
        df_spark,
        settings,
        SparkAPI(
            spark_session=spark,
            break_lineage_method="delta_lake_table",
        ),
    )

    df_predict = linker.inference.predict()

    _ = linker.clustering.cluster_pairwise_predictions_at_threshold(df_predict, 0.2)
