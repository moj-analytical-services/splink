import os

import pandas as pd

import splink.internals.comparison_library as cl
from splink import block_on
from splink.internals.linker import Linker

from .decorator import mark_with_dialects_excluding


@mark_with_dialects_excluding()
def test_model_heavily_customised_settings(test_helpers, dialect, tmp_path):
    helper = test_helpers[dialect]

    df_l = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
    # modify the frame to have source dataset column + different unique id name
    uid = "uid_col"
    source_ds_col = "dataset_name"
    df_l = df_l.rename(columns={"unique_id": uid})
    df_r = df_l.copy()
    df_l[source_ds_col] = "left_set"
    df_r[source_ds_col] = "right_set"

    # save temporarily to csv + read in again
    # older pyspark has issues with inferring types directly from pandas
    tmp_path_df_l = os.path.join(tmp_path, "df_l.csv")
    tmp_path_df_r = os.path.join(tmp_path, "df_r.csv")
    df_l.to_csv(tmp_path_df_l)
    df_r.to_csv(tmp_path_df_r)
    df_l = helper.load_frame_from_csv(tmp_path_df_l)
    df_r = helper.load_frame_from_csv(tmp_path_df_r)

    settings = {
        "link_type": "link_and_dedupe",
        "blocking_rules_to_generate_predictions": [
            block_on("city", "dob"),
            """l.first_name = r.first_name
            and l.surname = r.surname
            and substr(l.first_name, 1, 1) = 'j'
            """,
        ],
        "comparisons": [
            cl.ExactMatch("first_name"),
            cl.ExactMatch("surname"),
            cl.ExactMatch("city"),
            cl.ExactMatch("email"),
            cl.ExactMatch("dob"),
        ],
        "retain_intermediate_calculation_columns": True,
        "retain_matching_columns": True,
        "unique_id_column_name": uid,
        # not currently fully functional:
        # "source_dataset_column_name": source_ds_col,
        "additional_columns_to_retain": ["cluster"],
        "bayes_factor_column_prefix": "bayes_f__",
        "term_frequency_adjustment_column_prefix": "term_freq__",
        "comparison_vector_value_column_prefix": "cvv__",
    }
    linker = Linker([df_l, df_r], settings, **helper.extra_linker_args())
    # run through a few common operations to check functioning
    linker.training.estimate_probability_two_random_records_match(
        ["l.dob = r.dob"], 0.5
    )
    linker.training.estimate_u_using_random_sampling(2e4)
    linker.training.estimate_parameters_using_expectation_maximisation("l.dob = r.dob")
    df_predict = linker.inference.predict(0.1)
    df_clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(
        df_predict, 0.1
    )
    linker.visualisations.comparison_viewer_dashboard(
        df_predict, os.path.join(tmp_path, "csv.html")
    )
    linker.visualisations.cluster_studio_dashboard(
        df_predict, df_clusters, os.path.join(tmp_path, "csd.html")
    )
