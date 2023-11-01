import pandas as pd

import splink.comparison_level_library as cll

from .decorator import mark_with_dialects_excluding


@mark_with_dialects_excluding()
def test_cll_creators(dialect, test_helpers):
    helper = test_helpers[dialect]
    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    settings = {
        "link_type": "dedupe_only",
        "comparisons": [
            {
                "output_column_name": "first_name",
                "comparison_levels": [
                    cll.NullLevel("first_name"),
                    cll.ExactMatchLevel(
                        "first_name",
                        term_frequency_adjustments=True
                    ),
                    cll.LevenshteinLevel("first_name", 2),
                    cll.ElseLevel()
                ]
            },
            {
                "output_column_name": "surname",
                "comparison_levels": [
                    cll.NullLevel("surname"),
                    cll.ExactMatchLevel("surname", term_frequency_adjustments=True),
                    cll.LevenshteinLevel("surname", 2),
                    cll.ElseLevel()
                ]
            },
            {
                "output_column_name": "city",
                "comparison_levels": [
                    cll.NullLevel("city"),
                    cll.ExactMatchLevel("city", term_frequency_adjustments=True),
                    cll.LevenshteinLevel("city", 1),
                    cll.LevenshteinLevel("city", 2),
                    cll.ElseLevel()
                ]
            }
        ]
    }

    linker = helper.Linker(df, settings)
    linker.predict()
