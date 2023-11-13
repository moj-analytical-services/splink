import splink.comparison_level_library as cll
import splink.comparison_library as cl

from .decorator import mark_with_dialects_excluding

comparison_first_name = {
    "output_column_name": "first_name",
    "comparison_levels": [
        cll.NullLevel("first_name"),
        cll.ExactMatchLevel("first_name", term_frequency_adjustments=True),
        cll.LevenshteinLevel("first_name", 2).configure(m_probability=0.2),
        cll.ElseLevel(),
    ],
}
comparison_surname = {
    "output_column_name": "surname",
    "comparison_levels": [
        cll.NullLevel("surname"),
        cll.ExactMatchLevel("surname", term_frequency_adjustments=True),
        cll.LevenshteinLevel("surname", 2),
        cll.ElseLevel().configure(m_probability=0.2, u_probability=0.85),
    ],
}
comparison_city = {
    "output_column_name": "city",
    "comparison_levels": [
        cll.NullLevel("city"),
        cll.ExactMatchLevel("city", term_frequency_adjustments=True),
        cll.LevenshteinLevel("city", 1),
        cll.LevenshteinLevel("city", 2),
        cll.ElseLevel(),
    ],
}

cll_settings = {
    "link_type": "dedupe_only",
    "comparisons": [comparison_first_name, comparison_surname, comparison_city],
}


@mark_with_dialects_excluding()
def test_cll_creators_run_predict(dialect, test_helpers):
    helper = test_helpers[dialect]
    df = helper.load_frame_from_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    linker = helper.Linker(df, cll_settings, **helper.extra_linker_args())
    linker.predict()


@mark_with_dialects_excluding()
def test_cll_creators_instantiate_levels(dialect):
    cll.NullLevel("city").get_comparison_level(dialect)
    cll.ElseLevel().get_comparison_level(dialect)
    cll.ExactMatchLevel("city").get_comparison_level(dialect)
    cll.LevenshteinLevel("city", 5).get_comparison_level(dialect)


comparison_first_name = cl.LevenshteinAtThresholds("first_name", [2, 3])
comparison_surname = cl.LevenshteinAtThresholds("surname", [3])
comparison_city = cl.ExactMatch("city")
comparison_email = cl.LevenshteinAtThresholds("email", 3)

cl_settings = {
    "link_type": "dedupe_only",
    "comparisons": [
        comparison_first_name,
        comparison_surname,
        comparison_city,
        comparison_email,
    ],
}


@mark_with_dialects_excluding()
def test_cl_creators_run_predict(dialect, test_helpers):
    helper = test_helpers[dialect]
    df = helper.load_frame_from_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    linker = helper.Linker(df, cl_settings, **helper.extra_linker_args())
    linker.predict()
