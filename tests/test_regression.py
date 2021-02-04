from pyspark.sql import Row
from splink import Splink
from splink import case_statements
from splink.case_statements import _add_as_gamma_to_case_statement
import re


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
        "link_type": "dedupe_only",
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

    linker = Splink(settings, df, spark)
    df_e = linker.manually_apply_fellegi_sunter_weights()
    df = df_e.toPandas()
    assert df["match_probability"].min() > 0.0
    assert df["match_probability"].min() < 1e-20


def test_add_gamma():

    # See https://github.com/moj-analytical-services/splink/issues/174

    case_statement = """
    CASE
    WHEN my_col_l IS NULL
         OR my_col_r IS NULL THEN -1
    WHEN my_col_l = my_col_r THEN 1
    ELSE 0
    END as blah
    """

    stmt = _add_as_gamma_to_case_statement(case_statement, "my_col")
    assert "as blah" not in stmt
    assert re.search(r"END\s+as gamma_my_col", stmt)

    case_statement = """
    case
    when city_l is null or city_r is null then -1
    when city_l = city_r and city_l not in ('LONDON', 'BIRMINGHAM', 'LIVERPOOL', 'MANCHESTER') then 2
    when city_l = city_r then 1
    else 0 end
    """

    stmt = _add_as_gamma_to_case_statement(case_statement, "city")

    assert "'LONDON'" in stmt
    assert "as gamma_city" in stmt

    case_statement = """case
    when sname_l is null or sname_r is null then -1
    when sname_l = sname_r then 2
    when substr(sname_l,1, 3) =  substr(sname_r, 1, 3) then 1
    else 0
    end
    as gamma_sname
    """

    stmt = _add_as_gamma_to_case_statement(case_statement, "sname")
    assert len(re.findall("gamma_sname", stmt)) == 1

    case_statement = "case when a = 1 then 2 else 3 end as thing"

    stmt = _add_as_gamma_to_case_statement(case_statement, "sname")
    assert "thing" not in stmt
    assert re.search(r"case when a = 1 then 2 else 3 end\s+as gamma_sname", stmt)
