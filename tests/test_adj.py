from splink_data_generation.generate_data_random import generate_df_gammas_random
from splink_data_generation.match_prob import add_match_prob
from splink.default_settings import complete_settings_dict


def _probabilities_from_freqs(freqs, m_gamma_0=0.05, u_gamma_0=0.5):
    """Very roughly come up with some plausible u probabilities
    for given frequencies

    if true input nodes were [john,john,john,matt,matt,robin]
    then freqs would be [3,2,1]

    For johns, there are 9 comparisons and 3 matches
    For matts, there are 4 comparisons and 2 matches
    For robins there is  1 comparison and 1 match
    """

    # m probabilities
    m_probs = [f / sum(freqs) for f in freqs]
    adj = 1 - m_gamma_0
    m_probs = [f * adj for f in m_probs]
    m_probs.insert(0, m_gamma_0)

    u_probs = [f * f for f in freqs]
    u_probs = [f / sum(u_probs) for f in u_probs]
    adj = 1 - u_gamma_0
    u_probs = [f * adj for f in u_probs]
    u_probs.insert(0, u_gamma_0)

    return {
        "m_probabilities": m_probs,
        "u_probabilities": u_probs,
    }


def test_term_frequency_adjustments(spark):

    # The strategy is going to be to create a fake dataframe
    # where we have different levels to model frequency imbalance
    # gamma=3 is where name matches and name is robin (unusual name)
    # gamma=2 is where name matches and name is matt (normal name)
    # gamma=1 is where name matches and name is john (v common name)

    # We simulate the term frequency imbalance
    # by pooling this together, setting all gamma >0
    # to equal 1

    # We then expect that
    # term frequency adjustments should adjust up the
    # robins but adjust down the johns

    forename_probs = _probabilities_from_freqs([3, 2, 1])
    surname_probs = _probabilities_from_freqs([10, 5, 1])

    settings = {
        "link_type": "dedupe_only",
        "proportion_of_matches": 0.5,
        "comparison_columns": [
            {
                "col_name": "forename",
                "term_frequency_adjustments": True,
                "m_probabilities": forename_probs["m_probabilities"],
                "u_probabilities": forename_probs["u_probabilities"],
                "num_levels": 4,
            },
            {
                "col_name": "surname",
                "term_frequency_adjustments": True,
                "m_probabilities": surname_probs["m_probabilities"],
                "u_probabilities": surname_probs["u_probabilities"],
                "num_levels": 4,
            },
            {
                "col_name": "cat_20",
                "m_probabilities": [0.2, 0.8],
                "u_probabilities": [19 / 20, 1 / 20],
            },
        ],
        "em_convergence": 0.001,
    }

    settings = complete_settings_dict(settings, spark)

    # Create new binary columns that binarise the more granular gammas to 0 and 1
    df = generate_df_gammas_random(10000, settings)
    df["gamma_forename_binary"] = df["gamma_forename"].where(
        df["gamma_forename"] == 0, 1
    )
    df["gamma_surname_binary"] = df["gamma_surname"].where(df["gamma_surname"] == 0, 1)

    # Populate non matches with random value
    # Then assign left and right values ased on the gamma values
    df["forename_binary_l"] == df["unique_id_l"]
    df["forename_binary_r"] == df["unique_id_r"]

    f1 = df["gamma_forename"] == 3
    df.loc[f1, "forename_binary_l"] = "Robin"
    df.loc[f1, "forename_binary_r"] = "Robin"

    f1 = df["gamma_forename"] == 2
    df.loc[f1, "forename_binary_l"] = "Matt"
    df.loc[f1, "forename_binary_r"] = "Matt"

    f1 = df["gamma_forename"] == 1
    df.loc[f1, "forename_binary_l"] = "John"
    df.loc[f1, "forename_binary_r"] = "John"

    # Populate non matches with random value
    df["surname_binary_l"] == df["unique_id_l"]
    df["surname_binary_r"] == df["unique_id_r"]

    f1 = df["gamma_surname"] == 3
    df.loc[f1, "surname_binary_l"] = "Linacre"
    df.loc[f1, "surname_binary_r"] = "Linacre"

    f1 = df["gamma_surname"] == 2
    df.loc[f1, "surname_binary_l"] = "Hughes"
    df.loc[f1, "surname_binary_r"] = "Hughes"

    f1 = df["gamma_surname"] == 1
    df.loc[f1, "surname_binary_l"] = "Smith"
    df.loc[f1, "surname_binary_r"] = "Smith"

    settings["comparison_columns"][1]["term_frequency_adjustments"] = True

    from splink import Splink
    from splink.params import Params
    from splink.iterate import iterate
    from splink.term_frequencies import make_adjustment_for_term_frequencies

    # We have table of gammas - need to work from there within splink
    params = Params(settings, spark)

    df_e = iterate(df_gammas, params, settings, spark, compute_ll=False)

    df_e_adj = make_adjustment_for_term_frequencies(
        df_e, params, settings, retain_adjustment_columns=True, spark=spark
    )

    df_e_adj.createOrReplaceTempView("df_e_adj")
    sql = """
    select name_l, name_tf_adj,  count(*)
    from df_e_adj
    where name_l = name_r
    group by name_l, name_tf_adj
    order by name_l
    """
    df = spark.sql(sql).toPandas()
    df = df.set_index("name_l")
    df_dict = df.to_dict(orient="index")
    assert df_dict["a"]["name_tf_adj"] < 0.5

    assert df_dict["e"]["name_tf_adj"] > 0.5
    assert (
        df_dict["e"]["name_tf_adj"] > 0.6
    )  # Arbitrary numbers, but we do expect a big uplift here
    assert (
        df_dict["e"]["name_tf_adj"] < 0.95
    )  # Arbitrary numbers, but we do expect a big uplift here

    df_e_adj.createOrReplaceTempView("df_e_adj")
    sql = """
    select cat_12_l, cat_12_tf_adj,  count(*) as count
    from df_e_adj
    where cat_12_l = cat_12_r
    group by cat_12_l, cat_12_tf_adj
    order by cat_12_l
    """
    spark.sql(sql).toPandas()
    df = spark.sql(sql).toPandas()
    assert (
        df["cat_12_tf_adj"].max() < 0.55
    )  # Keep these loose because when generating random data anything can happen!
    assert df["cat_12_tf_adj"].min() > 0.45

    # Test adjustments applied coorrectly when there is one
    df_e_adj.createOrReplaceTempView("df_e_adj")
    sql = """
    select *
    from df_e_adj
    where name_l = name_r and cat_12_l != cat_12_r
    limit 1
    """
    df = spark.sql(sql).toPandas()
    df_dict = df.loc[0, :].to_dict()

    def bayes(p1, p2):
        return p1 * p2 / (p1 * p2 + (1 - p1) * (1 - p2))

    assert df_dict["tf_adjusted_match_prob"] == pytest.approx(
        bayes(df_dict["match_probability"], df_dict["name_tf_adj"])
    )

    # Test adjustments applied coorrectly when there are multiple
    df_e_adj.createOrReplaceTempView("df_e_adj")
    sql = """
    select *
    from df_e_adj
    where name_l = name_r and cat_12_l = cat_12_r
    limit 1
    """
    df = spark.sql(sql).toPandas()
    df_dict = df.loc[0, :].to_dict()

    double_b = bayes(
        bayes(df_dict["match_probability"], df_dict["name_tf_adj"]),
        df_dict["cat_12_tf_adj"],
    )

    assert df_dict["tf_adjusted_match_prob"] == pytest.approx(double_b)


@pytest.fixture(scope="module")
def nulls_df(spark):

    data = [
        {"unique_id": 1, "surname": "smith", "firstname": "john"},
        {"unique_id": 2, "surname": "smith", "firstname": "john"},
        {"unique_id": 3, "surname": "smithe", "firstname": "john"},
    ]

    dfpd = pd.DataFrame(data)
    df = spark.createDataFrame(dfpd)
    yield df


def test_freq_adj_divzero(spark, nulls_df):

    # create settings object that requests term_freq_adjustments on column 'weird'

    settings = {
        "link_type": "dedupe_only",
        "blocking_rules": [
            "l.surname = r.surname",
        ],
        "comparison_columns": [
            {
                "col_name": "firstname",
                "num_levels": 3,
            },
            {
                "col_name": "surname",
                "num_levels": 3,
                "term_frequency_adjustments": True,
            },
            {
                "col_name": "always_none",
                "num_levels": 3,
                "term_frequency_adjustments": True,
            },
        ],
        "additional_columns_to_retain": ["unique_id"],
    }

    # create column weird in a way that could trigger a div by zero on the average adj calculation before the fix
    nulls_df = nulls_df.withColumn("always_none", f.lit(None))

    try:
        linker = Splink(settings, spark, df=nulls_df)
        linker.get_scored_comparisons()
        notpassing = False
    except ZeroDivisionError:
        notpassing = True

    assert notpassing is False
