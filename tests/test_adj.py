import pytest
import pyspark.sql.functions as f
import pyspark
import warnings
import copy
import os

from splink import Splink, load_from_json
from splink.blocking import block_using_rules
from splink.params import Params
from splink.gammas import add_gammas, complete_settings_dict
from splink.iterate import iterate
from splink.expectation_step import run_expectation_step, get_overall_log_likelihood
from splink.case_statements import *
from splink.case_statements import _check_jaro_registered
import pandas as pd
from pandas.util.testing import assert_frame_equal
import logging






@pytest.fixture(scope="module")
def sparkdf(spark):       
        
    data = [
         {"surname": "smith", "firstname": "john"},
         {"surname": "smith", "firstname": "john"}, 
         {"surname": "smithe","firstname": "john"}


    ]
    
    dfpd = pd.DataFrame(data)
    df = spark.createDataFrame(dfpd)
    yield df
    
    
def test_freq_adj_divzero(spark, sparkdf):
    
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
            "term_frequency_adjustments": True
        },
        {
            "col_name": "weird",
            "num_levels": 3,
            "term_frequency_adjustments": True
        }
 
    ],
    "additional_columns_to_retain": ["unique_id"],
    "em_convergence": 0.01
    }
    
    
    sparkdf = sparkdf.withColumn("unique_id", f.monotonically_increasing_id())
    # create column weird in a way that could trigger a div by zero on the average adj calculation before the fix
    sparkdf = sparkdf.withColumn("weird",f.lit(None))
    
    
    try:
        linker = Splink(settings, spark, df=sparkdf)
        notpassing = False
    except ZeroDivisionError: 
        notpassing = True
        
    assert ( notpassing == False )
    
    
    
    
    

def test_term_frequency_adjustments(spark):


    settings = {
        "link_type": "dedupe_only",
        "proportion_of_matches": 0.1,
        "comparison_columns": [
            {
                "col_name": "name",
                "term_frequency_adjustments": True,
                "m_probabilities": [
                    0.1, # Amonst matches, 10% are have typose
                    0.9 # The reamining 90% have a match
                ],
                "u_probabilities": [
                    4/5, # Among non matches, 80% of the time there's no match
                    1/5 # But 20% of the time names 'collide'  WE WANT THESE U PROBABILITIES TO BE DEPENDENT ON NAME.  
                ],
            },
            {
                "col_name": "cat_12",
                "m_probabilities": [
                    0.05,
                    0.95
                ],
                "u_probabilities": [
                    11/12,
                    1/12
                ],
                
            },
            {
                "col_name": "cat_20",
                "m_probabilities": [
                    0.2,
                    0.8
                ],
                "u_probabilities": [
                    19/20,
                    1/20
                ],
            }
        ],
        "em_convergence": 0.001
    }


    from string import ascii_letters
    import statistics
    import random
    from splink.settings import complete_settings_dict
    settings = complete_settings_dict(settings, spark="supress_warnings")
    def is_match(settings):
        p = settings["proportion_of_matches"]
        return random.choices([0,1], [1-p, p])[0]

    def get_row_portion(match, comparison_col, skew="auto"):
        # Problem is that at the moment we're guaranteeing that a match on john is just as likely to be a match as a match on james
        
        # What we want is to generate more 'collisions' for john than robin i.e. if it's a non match, we want more gamma = 1 on name for john

        if match:
            gamma_pdist = comparison_col["m_probabilities"]
        else:
            gamma_pdist = comparison_col["u_probabilities"]
            
        
        # To decide whether gamma = 0 or 1 in the case of skew, we first need to decide on what value the left hand value column will take (well, what probability it has of selection)
        
        # How many distinct values should be choose?
        num_values = int(round(1/comparison_col["u_probabilities"][1]))
        
        if skew == "auto":
            skew = comparison_col["term_frequency_adjustments"]
            
        if skew:

            prob_dist = range(1,num_values+1)[::-1]  # a most freqent, last value least frequent
            # Normalise
            prob_dist = [p/sum(prob_dist) for p in prob_dist]
            
        
            index_of_value = random.choices(range(num_values), prob_dist)[0]
            if not match: # If it's a u probability
                this_prob = prob_dist[index_of_value]
                gamma_pdist = [1-this_prob, this_prob]
        
        else:
            prob_dist = [1/num_values]*num_values
            index_of_value = random.choices(range(num_values), prob_dist)[0]
            
            
        levels = comparison_col["num_levels"]
        gamma = random.choices(range(levels), gamma_pdist)[0]
        
        
        values = ascii_letters[:26] 
        if num_values > 26:
            values = [a + b for a in ascii_letters[:26] for b in ascii_letters[:26]] #aa, ab etc
            
        values = values[:num_values]

        if gamma == 1:
            value_1 = values[index_of_value]
            value_2 = value_1
            
        if gamma == 0:
            value_1 = values[index_of_value]
            same_value = True
            while same_value:
                value_2 = random.choices(values, prob_dist)[0]
                if value_1 != value_2:
                    same_value = False
            
        cname = comparison_col["col_name"]
        return {
            f"{cname}_l": value_1,
            f"{cname}_r": value_2,
            f"gamma_{cname}": gamma
        }    
        



    import uuid
    rows = []
    for uid in range(100000):
        m = is_match(settings)
        row = {"unique_id_l": str(uuid.uuid4()), "unique_id_r": str(uuid.uuid4()),  "match": m}
        for cc in settings["comparison_columns"]:
            row_portion = get_row_portion(m, cc)
            row = {**row, **row_portion}
        rows.append(row) 

    all_rows = pd.DataFrame(rows)
    df_gammas = spark.createDataFrame(all_rows)
    
    settings["comparison_columns"][1]["term_frequency_adjustments"] = True


    from splink import Splink
    from splink.params import Params 
    from splink.iterate import iterate
    from splink.term_frequencies import make_adjustment_for_term_frequencies

    # We have table of gammas - need to work from there within splink
    params = Params(settings, spark)

    df_e = iterate(
            df_gammas,
            params,
            settings,
            spark,
            compute_ll=False
        )

    df_e_adj = make_adjustment_for_term_frequencies(
            df_e,
            params,
            settings,
            retain_adjustment_columns=True,
            spark=spark
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
    df_dict = df.to_dict(orient='index')
    assert df_dict['a']["name_tf_adj"] < 0.5
    
    assert df_dict['e']["name_tf_adj"] > 0.5
    assert df_dict['e']["name_tf_adj"] > 0.6  #Arbitrary numbers, but we do expect a big uplift here
    assert df_dict['e']["name_tf_adj"] < 0.95 #Arbitrary numbers, but we do expect a big uplift here
    


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
    assert df["cat_12_tf_adj"].max() < 0.55 # Keep these loose because when generating random data anything can happen!
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
    df_dict = df.loc[0,:].to_dict()

    def bayes(p1, p2):
        return p1*p2 / (p1*p2 + (1-p1)*(1-p2))

    assert df_dict["tf_adjusted_match_prob"] ==  pytest.approx(bayes(df_dict["match_probability"], df_dict["name_tf_adj"]))
    

    # Test adjustments applied coorrectly when there are multiple
    df_e_adj.createOrReplaceTempView("df_e_adj")
    sql = """
    select *
    from df_e_adj
    where name_l = name_r and cat_12_l = cat_12_r
    limit 1
    """
    df = spark.sql(sql).toPandas()
    df_dict = df.loc[0,:].to_dict()

    double_b = bayes(bayes(df_dict["match_probability"], df_dict["name_tf_adj"]), df_dict["cat_12_tf_adj"])
    
    assert df_dict["tf_adjusted_match_prob"] ==  pytest.approx(double_b) 