from pytest import mark

import splink.comparison_library as cl
from splink import SettingsCreator

from .decorator import mark_with_dialects_excluding


# regression test: https://github.com/moj-analytical-services/splink/issues/3073
@mark.parametrize("debug_mode", [False, True])
@mark_with_dialects_excluding()
def test_pre_registered_tf_tables(dialect, test_helpers, debug_mode):
    helper = test_helpers[dialect]
    settings = SettingsCreator(
        link_type="dedupe_only",
        comparisons=[
            cl.ExactMatch("first_name").configure(term_frequency_adjustments=True),
            cl.ExactMatch("city").configure(term_frequency_adjustments=True),
        ],
        retain_intermediate_calculation_columns=True,
    )

    data = [
        {"unique_id": 1, "first_name": "Andy", "city": "London"},
        {"unique_id": 2, "first_name": "Andy", "city": "Liverpool"},
        {"unique_id": 3, "first_name": "Robin", "city": "Liverpool"},
    ]

    linker = helper.linker_with_registration([data], settings)

    linker._debug_mode = debug_mode
    city_tf = [
        {"city": "London", "tf_city": 0.2},
        {"city": "Liverpool", "tf_city": 0.8},
    ]

    linker.table_management.register_term_frequency_lookup(city_tf, "city")
    sdf_pred = linker.inference.predict()

    assert set(sdf_pred.as_dict()["tf_city_l"]) == {0.2, 0.8}
