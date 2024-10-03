import pandas as pd
from pytest import mark

import splink.comparison_library as cl
from splink import Linker, SettingsCreator, block_on

from .decorator import mark_with_dialects_excluding

df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
# we just want to check it runs, so use a small slice of the data
df = df[0:25]
df_l = df.copy()
df_r = df.copy()
df_m = df.copy()
df_l["source_dataset"] = "my_left_ds"
df_r["source_dataset"] = "my_right_ds"
df_m["source_dataset"] = "my_middle_ds"
df_combined = pd.concat([df_l, df_r])


@mark_with_dialects_excluding()
@mark.parametrize(
    ["link_type", "input_pd_tables"],
    [
        ["dedupe_only", [df]],
        ["link_only", [df, df]],  # no source dataset
        ["link_only", [df_l, df_r]],  # source dataset column
        ["link_only", [df_combined]],  # concatenated frame
        ["link_only", [df_l, df_m, df_r]],
        ["link_and_dedupe", [df, df]],  # no source dataset
        ["link_and_dedupe", [df_l, df_r]],  # source dataset column
        ["link_and_dedupe", [df_combined]],  # concatenated frame
    ],
    ids=[
        "dedupe",
        "link_only_no_source_dataset",
        "link_only_with_source_dataset",
        "link_only_concat",
        "link_only_three_tables",
        "link_and_dedupe_no_source_dataset",
        "link_and_dedupe_with_source_dataset",
        "link_and_dedupe_concat",
    ],
)
def test_clustering(test_helpers, dialect, link_type, input_pd_tables):
    helper = test_helpers[dialect]

    settings = SettingsCreator(
        link_type=link_type,
        comparisons=[
            cl.ExactMatch("first_name"),
            cl.ExactMatch("surname"),
            cl.ExactMatch("dob"),
            cl.ExactMatch("city"),
        ],
        blocking_rules_to_generate_predictions=[
            block_on("surname"),
            block_on("dob"),
        ],
    )
    linker_input = list(map(helper.convert_frame, input_pd_tables))
    linker = Linker(linker_input, settings, **helper.extra_linker_args())

    df_predict = linker.inference.predict()
    linker.clustering.cluster_pairwise_predictions_at_threshold(df_predict, 0.95)
