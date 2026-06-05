import os

import pyarrow as pa

import splink.internals.comparison_library as cl
from splink import block_on

from .decorator import mark_with_dialects_excluding


@mark_with_dialects_excluding()
def test_model_heavily_customised_settings(test_helpers, dialect, tmp_path, fake_1000):
    helper = test_helpers[dialect]

    # modify the frame to have source dataset column + different unique id name
    uid = "uid_col"
    source_ds_col = "dataset_name"
    df_l = fake_1000.rename_columns({"unique_id": uid})
    df_r = df_l.append_column(source_ds_col, pa.array(1000 * ["right_set"]))
    df_l = df_l.append_column(source_ds_col, pa.array(1000 * ["left_set"]))

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
        "source_dataset_column_name": source_ds_col,
        "additional_columns_to_retain": ["cluster"],
        "match_weight_column_prefix": "match_w__",
        "term_frequency_adjustment_column_prefix": "term_freq__",
        "comparison_vector_value_column_prefix": "cvv__",
    }
    linker = helper.linker_with_registration([df_l, df_r], settings)
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
