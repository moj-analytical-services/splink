from pyspark.sql import Row
from splink import Splink


def test_tiny_numbers(spark):

    rows = [
        {"unique_id": 1, "mob": 10, "surname": "Linacre"},
        {"unique_id": 2, "mob": 10, "surname": "Linacre"},
        {"unique_id": 3, "mob": 10, "surname": "Linacer"},
        {"unique_id": 4, "mob": 7, "surname": "Smith"},
        {"unique_id": 5, "mob": 8, "surname": "Smith"},
        {"unique_id": 6, "mob": 8, "surname": "Smith"},
        {"unique_id": 7, "mob": 8, "surname": "Jones"},
    ]

    df = spark.createDataFrame(Row(**x) for x in rows)

    # Regression test, see https://github.com/moj-analytical-services/splink/issues/48

    settings = {
        "link_type": "link_and_dedupe",
        "proportion_of_matches": 0.4,
        "comparison_columns": [
            {
                "col_name": "mob",
                "num_levels": 2,
                "m_probabilities": [5.9380419956766985e-25, 1 - 5.9380419956766985e-25],
                "u_probabilities": [0.8, 0.2],
            },
            {
                "col_name": "surname",
                "num_levels": 2,
            },
        ],
        "blocking_rules": [
            "l.mob = r.mob",
            "l.surname = r.surname",
        ],
    }

    linker = Splink(settings, spark, df=df)
    df_e = linker.manually_apply_fellegi_sunter_weights()
    df = df_e.toPandas()
    assert df["match_probability"].min() > 0.0
    assert df["match_probability"].min() < 1e-20
