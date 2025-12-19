import math

import pandas as pd

import splink.comparison_library as cl
from splink import DuckDBAPI, Linker, SettingsCreator


def _create_extreme_comparisons(
    m_match: float, u_match: float, m_else: float, u_else: float, num_cols: int = 5
) -> list:
    """Create comparisons with extreme m/u probability values."""
    comparisons = []
    for i in range(1, num_cols + 1):
        col = f"c{i}"
        comparisons.append(
            cl.ExactMatch(col).configure(
                m_probabilities=[m_match, m_else],
                u_probabilities=[u_match, u_else],
            )
        )
    return comparisons


def test_extreme_match_weights_high_similarity():
    data = [
        {"id": 1, "c1": "a", "c2": "a", "c3": "a", "c4": "a", "c5": "a"},
        {"id": 2, "c1": "a", "c2": "a", "c3": "a", "c4": "a", "c5": "a"},
    ]
    df = pd.DataFrame(data)

    # Each exact match level has BF = m/u = 0.999 / 1e-300 ≈ 1e92
    # 5 columns * 92 = 460 > 308, so would overflow without clamping
    comparisons = _create_extreme_comparisons(
        m_match=0.999,
        u_match=1e-300,
        m_else=1e-300,
        u_else=0.999,
    )

    settings = SettingsCreator(
        link_type="dedupe_only",
        unique_id_column_name="id",
        blocking_rules_to_generate_predictions=["1=1"],
        comparisons=comparisons,
        retain_intermediate_calculation_columns=True,
    )

    db_api = DuckDBAPI()
    df_sdf = db_api.register(df)
    linker = Linker(df_sdf, settings)
    predictions = linker.inference.predict().as_pandas_dataframe()

    # Should get exactly one prediction (comparing record 1 with record 2)
    assert len(predictions) == 1

    # Match probability should be very high (close to 1.0)
    match_prob = predictions["match_probability"].iloc[0]
    assert match_prob == 1.0, f"Expected very high match probability, got {match_prob}"

    # Match weight should be finite (not inf or nan)
    match_weight = predictions["match_weight"].iloc[0]
    assert math.isfinite(match_weight), f"Match weight is not finite: {match_weight}"


def test_extreme_match_weights_low_similarity():
    data = [
        {"id": 1, "c1": "a", "c2": "a", "c3": "a", "c4": "a", "c5": "a"},
        {"id": 2, "c1": "b", "c2": "b", "c3": "b", "c4": "b", "c5": "b"},
    ]
    df = pd.DataFrame(data)

    comparisons = _create_extreme_comparisons(
        m_match=0.999,
        u_match=1e-300,
        m_else=1e-300,
        u_else=0.999,
    )

    settings = SettingsCreator(
        link_type="dedupe_only",
        unique_id_column_name="id",
        blocking_rules_to_generate_predictions=["1=1"],
        comparisons=comparisons,
        retain_intermediate_calculation_columns=True,
    )

    db_api = DuckDBAPI()
    df_sdf = db_api.register(df)
    linker = Linker(df_sdf, settings)
    predictions = linker.inference.predict().as_pandas_dataframe()

    match_prob = predictions["match_probability"].iloc[0]
    assert match_prob < 1e-300, f"Expected very low match probability, got {match_prob}"

    # Match weight should be finite (not -inf or nan)
    match_weight = predictions["match_weight"].iloc[0]
    assert math.isfinite(match_weight), f"Match weight is not finite: {match_weight}"
