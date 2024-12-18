import pandas as pd
import pytest
from pytest import mark

import splink.comparison_library as cl
from splink import DuckDBAPI, Linker, SettingsCreator, block_on

from .basic_settings import get_settings_dict
from .decorator import mark_with_dialects_excluding

from splink import DuckDBAPI, Linker

@mark_with_dialects_excluding()
def test_single_best_links_correctness(test_helpers, dialect):
    helper = test_helpers[dialect]

    df = pd.DataFrame({
        "unique_id":      [  0,  1,   2,   3,   4,   5,   6,   7,   8],
        "source_dataset": ['a', 'b', 'c', 'a', 'b', 'c', 'a', 'b', 'c'],
    })

    predictions = pd.DataFrame({
        "unique_id_l":       [  0,   1,   3,   4,   6,   6],
        "unique_id_r":       [  1,   2,   5,   5,   5,   7],
        "source_dataset_l":  ['a', 'b', 'a', 'b', 'a', 'a'],
        "source_dataset_r":  ['b', 'c', 'c', 'c', 'c', 'b'],
        "match_probability": [.90, .70, .85, .90, .80, .70],
    })

    settings = SettingsCreator(
        link_type="link_only",
        comparisons=[],
        blocking_rules_to_generate_predictions=[],
    )

    linker = Linker(df, settings, **helper.extra_linker_args())

    df_predict = linker.table_management.register_table_predict(predictions, overwrite=True)

    df_clusters = linker.clustering.cluster_single_best_links_at_threshold(
        df_predict, 
        source_datasets=["a", "b", "c"], 
        threshold_match_probability=0.5
    )

    result = df_clusters.as_pandas_dataframe().sort_values("unique_id")
    result = result.reset_index(drop=True)

    correct_result = pd.DataFrame({
        "cluster_id": ["a-__-0", "a-__-0", "a-__-0", "a-__-3", "a-__-3", "a-__-3", "a-__-6", "b-__-7", "c-__-8"],
        "unique_id":      [  0,  1,   2,   3,   4,   5,   6,   7,   8],
        "source_dataset": ['a', 'b', 'c', 'a', 'b', 'c', 'a', 'b', 'c'],
    })
    correct_result = correct_result.sort_values("unique_id")
    correct_result = correct_result.reset_index(drop=True)

    pd.testing.assert_frame_equal(result, correct_result)

