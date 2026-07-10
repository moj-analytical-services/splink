import logging

import pytest

from splink.internals.comparison_level import ComparisonLevel
from splink.internals.comparison_level_library import ExactMatchLevel
from splink.internals.dialects import SplinkDialect
from splink.internals.duckdb.database_api import DuckDBAPI
from splink.internals.linker import Linker


def _level(**kwargs):
    return ComparisonLevel(
        sql_condition=kwargs.pop("sql_condition", "ELSE"),
        label_for_charts=kwargs.pop("label_for_charts", "Test level"),
        sql_dialect=SplinkDialect.from_string("duckdb"),
        **kwargs,
    )


def _settings():
    return {
        "link_type": "dedupe_only",
        "probability_two_random_records_match": 0.5,
        "comparisons": [
            {
                "output_column_name": "name",
                "comparison_levels": [
                    {
                        "sql_condition": "name_l IS NULL OR name_r IS NULL",
                        "label_for_charts": "Null",
                        "is_null_level": True,
                        "match_weight": 1.25,
                    },
                    {
                        "sql_condition": "name_l = name_r",
                        "label_for_charts": "Exact",
                        "match_weight": 3.0,
                    },
                    {
                        "sql_condition": "ELSE",
                        "label_for_charts": "Else",
                        "m_probability": 0.2,
                        "u_probability": 0.8,
                    },
                ],
            }
        ],
        "blocking_rules_to_generate_predictions": ["1=1"],
        "retain_intermediate_calculation_columns": True,
    }


def _data():
    return [
        {"unique_id": 1, "name": None, "cluster": 1},
        {"unique_id": 2, "name": None, "cluster": 1},
        {"unique_id": 3, "name": "Robin", "cluster": 2},
        {"unique_id": 4, "name": "Robin", "cluster": 2},
        {"unique_id": 5, "name": "James", "cluster": 3},
    ]


def _linker():
    db_api = DuckDBAPI()
    return Linker(db_api.register(_data()), _settings()), db_api


@pytest.mark.parametrize("match_weight", [0, 3.5, -4.25])
def test_match_weight_mode_and_serialization(match_weight):
    level = _level(match_weight=match_weight)

    assert level._is_match_weight_mode
    assert level.match_weight == match_weight
    assert level.m_probability is None
    assert level.u_probability is None
    assert level._m_is_trained
    assert level._u_is_trained
    assert level.as_dict() == {
        "sql_condition": "ELSE",
        "label_for_charts": "Test level",
        "match_weight": match_weight,
    }


def test_match_weight_can_be_used_on_null_level():
    level = _level(match_weight=1.5, is_null_level=True)

    assert level.match_weight == 1.5
    assert level._match_weight == 1.5
    assert level._log2_bayes_factor == 1.5
    assert level.m_probability is None
    assert level.u_probability is None


@pytest.mark.parametrize(
    "kwargs, message",
    [
        ({"match_weight": float("nan")}, "finite number"),
        ({"match_weight": float("inf")}, "finite number"),
        ({"match_weight": "3"}, "finite number"),
        ({"match_weight": True}, "finite number"),
        (
            {"match_weight": 3, "m_probability": 0.9},
            "cannot be combined with m_probability or u_probability",
        ),
        (
            {"match_weight": 3, "u_probability": 0.1},
            "cannot be combined with m_probability or u_probability",
        ),
        (
            {"match_weight": 3, "fix_m_probability": True},
            "cannot be combined with fix_m_probability or fix_u_probability",
        ),
        (
            {"match_weight": 3, "fix_u_probability": True},
            "cannot be combined with fix_m_probability or fix_u_probability",
        ),
    ],
)
def test_invalid_match_weight_settings(kwargs, message):
    with pytest.raises(ValueError, match=message):
        _level(**kwargs)


def test_match_weight_with_tf_warns_and_errors(caplog):
    with caplog.at_level(logging.WARNING), pytest.raises(
        ValueError, match="term-frequency adjustments"
    ):
        _level(match_weight=3, tf_adjustment_column="name")

    assert "TF adjustments require explicit" in caplog.text


def test_creator_configure_supports_match_weight():
    level_dict = (
        ExactMatchLevel("name")
        .configure(match_weight=3)
        .create_level_dict("duckdb")
    )

    assert level_dict["match_weight"] == 3
    assert "m_probability" not in level_dict
    assert "u_probability" not in level_dict


def test_raw_settings_predict_exact_weights_and_round_trip():
    linker, _ = _linker()
    records = linker.inference.predict().as_record_list()
    weights_by_ids = {
        (record["unique_id_l"], record["unique_id_r"]): record["mw_name"]
        for record in records
    }

    assert weights_by_ids[(1, 2)] == 1.25
    assert weights_by_ids[(3, 4)] == 3.0
    assert weights_by_ids[(3, 5)] == -2.0

    serialized = linker._settings_obj.as_dict()
    levels = serialized["comparisons"][0]["comparison_levels"]
    assert levels[0]["match_weight"] == 1.25
    assert levels[1]["match_weight"] == 3.0
    for level in levels[:2]:
        assert "m_probability" not in level
        assert "u_probability" not in level
        assert "fix_m_probability" not in level
        assert "fix_u_probability" not in level

    db_api = DuckDBAPI()
    reloaded = Linker(db_api.register(_data()), serialized)
    reloaded_records = reloaded.inference.predict().as_record_list()
    reloaded_weights = {
        (record["unique_id_l"], record["unique_id_r"]): record["mw_name"]
        for record in reloaded_records
    }
    assert reloaded_weights == weights_by_ids


@pytest.mark.parametrize(
    "training_method",
    ["estimate_u", "em", "m_from_column", "m_from_pairwise_labels"],
)
def test_training_never_overwrites_match_weight(training_method):
    linker, db_api = _linker()

    if training_method == "estimate_u":
        linker.training.estimate_u_using_random_sampling(max_pairs=100, num_chunks=1)
    elif training_method == "em":
        linker.training.estimate_parameters_using_expectation_maximisation("1=1")
    elif training_method == "m_from_column":
        linker.training.estimate_m_from_label_column("cluster")
    else:
        labels = [
            {
                "source_dataset_l": "fake_data_1",
                "source_dataset_r": "fake_data_1",
                "unique_id_l": left["unique_id"],
                "unique_id_r": right["unique_id"],
            }
            for left in _data()
            for right in _data()
            if left["cluster"] == right["cluster"]
            and left["unique_id"] < right["unique_id"]
        ]
        db_api.register(labels, table_name="labels")
        linker.training.estimate_m_from_pairwise_labels("labels")

    level = linker._settings_obj.comparisons[0].comparison_levels[1]
    assert level.match_weight == 3.0
    assert level.m_probability is None
    assert level.u_probability is None
    assert level._trained_m_probabilities == []
    assert level._trained_u_probabilities == []
    assert level.as_dict()["match_weight"] == 3.0


def test_existing_m_u_mode_is_unchanged():
    level = _level(m_probability=0.6, u_probability=0.2)

    assert not level._is_match_weight_mode
    assert level.m_probability == 0.6
    assert level.u_probability == 0.2
    assert level.match_weight == pytest.approx(1.584962500721156)
    assert level.as_dict()["m_probability"] == 0.6
    assert level.as_dict()["u_probability"] == 0.2
