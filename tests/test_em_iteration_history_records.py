from splink.internals.comparison_level import ComparisonLevelDetailedRecord
from splink.internals.em_training_session import (
    EMTrainingSession,
    ModelParameterDetailedRecord,
)


def _parameter_detailed_record() -> ModelParameterDetailedRecord:
    cl_rec = ComparisonLevelDetailedRecord(
        sql_condition="l.first_name = r.first_name",
        label_for_charts="Exact match on first_name",
        has_tf_adjustments=False,
        tf_adjustment_column=None,
        tf_adjustment_weight=None,
        is_null_level=False,
        m_probability=0.9,
        u_probability=0.1,
        m_probability_description=None,
        u_probability_description=None,
        bayes_factor=9.0,
        log2_bayes_factor=3.17,
        bayes_factor_description="",
        comparison_vector_value=1,
        max_comparison_vector_value=2,
        comparison_name="first_name",
    )
    return ModelParameterDetailedRecord.from_cl_detailed_record(
        cl_rec,
        probability_two_random_records_match=0.5,
        comparison_sort_order=0,
    )


class _StubCoreModelSettings:
    def __init__(self, probability_two_random_records_match):
        self.probability_two_random_records_match = (
            probability_two_random_records_match
        )
        self.parameters_as_detailed_records = [_parameter_detailed_record()]


def _session_with_history(lam_history):
    # EMTrainingSession.__init__ needs a live linker and database, which the
    # iteration history records do not use; build the instance directly.
    session = EMTrainingSession.__new__(EMTrainingSession)
    session._core_model_settings_history = [
        _StubCoreModelSettings(lam) for lam in lam_history
    ]
    # The final (converged) settings, as set by _train after the EM loop.
    session.core_model_settings = _StubCoreModelSettings(lam_history[-1])
    return session


def test_iteration_history_records_use_each_iterations_probability():
    # https://github.com/moj-analytical-services/splink/issues/1619
    # Every iteration's records must carry that iteration's probability, not
    # the final converged value, otherwise the m/u and match weights history
    # chart tooltips report the wrong lambda for all but the last iteration.
    lam_history = [0.001, 0.002, 0.004]

    session = _session_with_history(lam_history)

    records = session._iteration_history_records

    assert [r.iteration for r in records] == [0, 1, 2]
    assert [r.probability_two_random_records_match for r in records] == lam_history


def test_iteration_history_records_single_iteration_matches_final_probability():
    lam_history = [0.01]

    session = _session_with_history(lam_history)

    records = session._iteration_history_records

    assert len(records) == 1
    assert records[0].probability_two_random_records_match == 0.01
    assert records[0].iteration == 0
