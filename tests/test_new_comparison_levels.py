import splink.comparison_level_library as cll

from .decorator import mark_with_dialects_excluding

comparison_first_name = {
    "output_column_name": "first_name",
    "comparison_levels": [
        cll.NullLevel("first_name"),
        cll.ExactMatchLevel("first_name",
            # term_frequency_adjustments=True
        ),
        # cll.LevenshteinLevel("first_name", 2),
        cll.ElseLevel()
    ]
}
comparison_surname = {
    "output_column_name": "surname",
    "comparison_levels": [
        cll.NullLevel("surname"),
        cll.ExactMatchLevel("surname"),# term_frequency_adjustments=True),
        # cll.LevenshteinLevel("surname", 2),
        cll.ElseLevel()
    ]
}
comparison_city = {
    "output_column_name": "city",
    "comparison_levels": [
        cll.NullLevel("city"),
        cll.ExactMatchLevel("city"),# term_frequency_adjustments=True),
        # cll.LevenshteinLevel("city", 1),
        # cll.LevenshteinLevel("city", 2),
        cll.ElseLevel()
    ]
}

settings = {
    "link_type": "dedupe_only",
    "comparisons": [
        comparison_first_name,
        comparison_surname,
        comparison_city
    ]
}

@mark_with_dialects_excluding()
def test_cll_creators_run_predict(dialect, test_helpers):
    helper = test_helpers[dialect]
    df = helper.load_frame_from_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    linker = helper.Linker(df, settings, **helper.extra_linker_args())
    linker.predict()

@mark_with_dialects_excluding()
def test_cll_creators_instantiate_levels(dialect):
    cll.NullLevel("city").get_comparison_level(dialect)
    cll.ElseLevel().get_comparison_level(dialect)
    cll.ExactMatchLevel("city").get_comparison_level(dialect)
    # cll.LevenshteinLevel("city", 5).get_comparison_level(dialect)
