import pandas as pd

from .decorator import mark_with_dialects_excluding


@mark_with_dialects_excluding()
def test_model_heavily_customised_settings(test_helpers, dialect):
    helper = test_helpers[dialect]

    df_l = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
    uid = "uid_col"
    source_ds_col = "dataset_name"
    df_l = df_l.rename(columns={"unique_id": uid})
    df_r = df_l.copy()
    df_l[source_ds_col] = "left_set"
    df_r[source_ds_col] = "right_set"

    df_l = helper.convert_frame(df_l)
    df_r = helper.convert_frame(df_r)

    settings = {
        "link_type": "link_and_dedupe",
        "blocking_rules_to_generate_predictions": [
            "l.city = r.city AND l.dob = r.dob",
            "l.first_name = r.first_name and l.surname = r.surname",
        ],
        "comparisons": [
            helper.cl.exact_match("first_name"),
            helper.cl.exact_match("surname"),
            helper.cl.exact_match("city"),
            helper.cl.exact_match("email"),
            helper.cl.exact_match("dob"),
        ],
        "unique_id_column_name": uid,
        # not currently fully functional:
        # "source_dataset_column_name": source_ds_col,
        "additional_columns_to_retain": ["group"],
        "bayes_factor_column_prefix": "bayes_f__",
        "term_frequency_adjustment_column_prefix": "term_freq__",
        "comparison_vector_value_column_prefix": "cvv__",
    }
    linker = helper.Linker([df_l, df_r], settings, **helper.extra_linker_args())
    # run through a few common operations to check functioning
    linker.estimate_probability_two_random_records_match("l.dob = r.dob", 0.5)
    linker.estimate_u_using_random_sampling(2e4)
    linker.estimate_parameters_using_expectation_maximisation("l.dob = r.dob")
    df_predict = linker.predict(0.1)
    linker.cluster_pairwise_predictions_at_threshold(df_predict, 0.1)
