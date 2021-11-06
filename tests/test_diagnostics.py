import pandas as pd
import random
import pytest

from splink.diagnostics import (
    splink_score_histogram,
    _calc_probability_density,
    _create_probability_density_plot,
)
import pytest
from pyspark.sql import Row


@pytest.fixture(scope="module")
def df(spark):

    rows = []
    for i in range(1000):
        r = {
            "match_probability": random.uniform(0, 1),
            "tf_adjusted_match_prob": random.uniform(0, 1),
        }
        rows.append(r)

    df = spark.createDataFrame(Row(**x) for x in rows)
    yield df


def test_score_hist_splits(spark, df):
    """
    test that a dataframe gets processed with non-default splits list
    test binwidths and normalised probability densities sum up to 1.0
    """

    mysplits = [0.3, 0.6]

    res = _calc_probability_density(df, spark=spark, buckets=mysplits)
    res = pd.DataFrame(res)

    assert res.count_rows.count() == 3
    assert res.count_rows.sum() == res.count_rows.cumsum()[2]
    assert res.binwidth.sum() == pytest.approx(1.0)
    assert res.normalised.sum() == pytest.approx(1.0)

    mysplits2 = [0.6, 0.3]

    res2 = _calc_probability_density(df, spark=spark, buckets=mysplits2)
    res2 = pd.DataFrame(res2)

    assert res2.count_rows.count() == 3
    assert res2.count_rows.sum() == res.count_rows.cumsum()[2]
    assert res2.binwidth.sum() == pytest.approx(1.0)
    assert res2.normalised.sum() == pytest.approx(1.0)


def test_score_hist_intsplits(spark, df):
    """
    test integer value in splits variable
    """

    res3 = _calc_probability_density(df, spark=spark, buckets=5)
    res3 = pd.DataFrame(res3)
    assert res3.count_rows.count() == 5
    assert res3.binwidth.sum() == pytest.approx(1.0)
    assert res3.normalised.sum() == pytest.approx(1.0)


def test_score_hist_output_json(spark, df):
    """
    test chart exported as dictionary is in fact a valid dictionary
    """

    altair_installed = True
    try:
        import altair as alt
    except ImportError:
        altair_installed = False

    res4 = _calc_probability_density(df, spark=spark, buckets=5)

    if altair_installed:
        assert isinstance(_create_probability_density_plot(res4).to_dict(), dict)
    else:
        assert isinstance(_create_probability_density_plot(res4), dict)


def test_prob_density(spark, df):

    """
    a test that checks that probability density is computed correctly.
    explicitly define a dataframe with tf_adjusted_match_prob = [0.1, 0.3, 0.5, 0.7, 0.9]
    and make sure that the probability density is the correct value (0.2) with all 5 bins
    """

    dfpd = pd.DataFrame([0.1, 0.3, 0.5, 0.7, 0.9], columns=["match_probability"])
    spdf = spark.createDataFrame(dfpd)

    res = _calc_probability_density(spdf, spark=spark, buckets=5)
    res = pd.DataFrame(res)
    assert all(value == pytest.approx(0.2) for value in res.normalised.values)
