import os

from splink.blocking_analysis import (
    chart_comparisons_from_blocking_rules,
)

from .decorator import mark_with_dialects_excluding


@mark_with_dialects_excluding()
def test_deterministic_link_full_example(fake_1000, dialect, tmp_path, test_helpers):
    helper = test_helpers[dialect]

    br_for_predict = [
        "l.first_name = r.first_name and l.surname = r.surname and l.dob = r.dob",
        "l.surname = r.surname and l.dob = r.dob and l.email = r.email",
        "l.first_name = r.first_name and l.surname = r.surname "
        "and l.email = r.email",
    ]
    settings = {
        "link_type": "dedupe_only",
        "blocking_rules_to_generate_predictions": br_for_predict,
        "retain_matching_columns": True,
        "retain_intermediate_calculation_columns": True,
    }
    helper_db_api = helper.db_api()
    df_sdf = helper_db_api.register(fake_1000)

    chart_comparisons_from_blocking_rules(
        df_sdf,
        blocking_rules=br_for_predict,
        link_type="dedupe_only",
        unique_id_column_name="unique_id",
    )

    linker = helper.linker_with_registration(fake_1000, settings)

    df_predict = linker.inference.deterministic_link()

    clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(df_predict)

    linker.visualisations.cluster_studio_dashboard(
        df_predict,
        clusters,
        out_path=os.path.join(tmp_path, "test_cluster_studio.html"),
        sampling_method="by_cluster_size",
        overwrite=True,
    )
