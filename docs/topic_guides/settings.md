---
tags:
  - settings
  - Dedupe
  - Link
  - Link and Dedupe
  - Comparisons
  - Blocking Rules
---

# Defining a Splink Model

## What makes a Splink Model?

When building any linkage model in Splink, there are 3 key things which need to be defined:

1. What **type of linkage** you want (defined by the [link type](link_type.md))
2. What **pairs of records** to consider (defined by [blocking rules](./blocking_rules.md))
3. What **features** to consider, and how they should be **compared** (defined by [comparisons](./customising_comparisons.ipynb))


## Defining a Splink model with a settings dictionary

All aspects of a Splink model are defined via the settings dictionary. The settings object is a json-like object which underpins a model.

For example, consider a simple model (as defined in the [README](https://github.com/moj-analytical-services/splink#quickstart)). The model is defined by the following settings dictionary

```py linenums="1"
import splink.duckdb.comparison_library as cl
import splink.duckdb.comparison_template_library as ctl

settings = {
    "link_type": "dedupe_only",
    "blocking_rules_to_generate_predictions": [
        "l.first_name = r.first_name",
        "l.surname = r.surname",
    ],
    "comparisons": [
        ctl.name_comparison("first_name"),
        ctl.name_comparison("surname"),
        ctl.date_comparison("dob", cast_strings_to_date=True),
        cl.exact_match("city", term_frequency_adjustments=True),
        ctl.email_comparison("email"),
    ],
}
```

Where:

**1. Type of linkage**

The `"link_type"` is defined as a deduplication for a single dataset.

```py linenums="5"
    "link_type": "dedupe_only",
```

**2. Pairs of records to consider**

The `"blocking_rules_to_generate_predictions"` define a subset of pairs of records for the model to be trained on where there is a match on `"first_name"` or `"surname"`.

```py linenums="6"
    "blocking_rules_to_generate_predictions": [
        "l.first_name = r.first_name",
        "l.surname = r.surname",
    ],
```

**3. Features to consider, and how they should be compared**

The `"comparisons"` define the features to be compared between records: `"first_name"`, `"surname"`, `"dob"`, `"city"` and `"email"`.

```py linenums="10"
    "comparisons": [
        ctl.name_comparison("first_name"),
        ctl.name_comparison("surname"),
        ctl.date_comparison("dob", cast_strings_to_date=True),
        cl.exact_match("city", term_frequency_adjustments=True),
        ctl.email_comparison("email"),
    ],
```

Using functions from the [comparison template library](comparison_templates.ipynb) and [comparison library](./customising_comparisons.ipynb#method-1-using-the-comparisonlibrary) to define **how** these features should be compared.

```py linenums="1"
import splink.duckdb.comparison_library as cl
import splink.duckdb.comparison_template_library as ctl
```

For more information on how comparisons are defined, see the [dedicated topic guide](./customising_comparisons.ipynb).

These functions generate comparisons within the settings dictionary. See below for the full settings dictionary once the `comparison_library` and `comparison_template_library` functions have been evaluated and constructed:

??? info "Settings Dictionary in full"

    ```json
    {
        "link_type": "dedupe_only",
        "blocking_rules_to_generate_predictions": [
            "l.first_name = r.first_name",
            "l.surname = r.surname"
        ],
        "comparisons": [
            {
                "output_column_name": "first_name",
                "comparison_levels": [
                    {
                        "sql_condition": "\"first_name_l\" IS NULL OR \"first_name_r\" IS NULL",
                        "label_for_charts": "Null",
                        "is_null_level": true
                    },
                    {
                        "sql_condition": "\"first_name_l\" = \"first_name_r\"",
                        "label_for_charts": "Exact match first_name"
                    },
                    {
                        "sql_condition": "damerau_levenshtein(\"first_name_l\", \"first_name_r\") <= 1",
                        "label_for_charts": "Damerau_levenshtein <= 1"
                    },
                    {
                        "sql_condition": "jaro_winkler_similarity(\"first_name_l\", \"first_name_r\") >= 0.9",
                        "label_for_charts": "Jaro_winkler_similarity >= 0.9"
                    },
                    {
                        "sql_condition": "jaro_winkler_similarity(\"first_name_l\", \"first_name_r\") >= 0.8",
                        "label_for_charts": "Jaro_winkler_similarity >= 0.8"
                    },
                    {
                        "sql_condition": "ELSE",
                        "label_for_charts": "All other comparisons"
                    }
                ],
                "comparison_description": "Exact match vs. First_Name within levenshtein threshold 1 vs. First_Name within damerau-levenshtein threshold 1 vs. First_Name within jaro_winkler thresholds 0.9, 0.8 vs. anything else"
            },
            {
                "output_column_name": "surname",
                "comparison_levels": [
                    {
                        "sql_condition": "\"surname_l\" IS NULL OR \"surname_r\" IS NULL",
                        "label_for_charts": "Null",
                        "is_null_level": true
                    },
                    {
                        "sql_condition": "\"surname_l\" = \"surname_r\"",
                        "label_for_charts": "Exact match surname"
                    },
                    {
                        "sql_condition": "damerau_levenshtein(\"surname_l\", \"surname_r\") <= 1",
                        "label_for_charts": "Damerau_levenshtein <= 1"
                    },
                    {
                        "sql_condition": "jaro_winkler_similarity(\"surname_l\", \"surname_r\") >= 0.9",
                        "label_for_charts": "Jaro_winkler_similarity >= 0.9"
                    },
                    {
                        "sql_condition": "jaro_winkler_similarity(\"surname_l\", \"surname_r\") >= 0.8",
                        "label_for_charts": "Jaro_winkler_similarity >= 0.8"
                    },
                    {
                        "sql_condition": "ELSE",
                        "label_for_charts": "All other comparisons"
                    }
                ],
                "comparison_description": "Exact match vs. Surname within levenshtein threshold 1 vs. Surname within damerau-levenshtein threshold 1 vs. Surname within jaro_winkler thresholds 0.9, 0.8 vs. anything else"
            },
            {
                "output_column_name": "dob",
                "comparison_levels": [
                    {
                        "sql_condition": "\"dob_l\" IS NULL OR \"dob_r\" IS NULL",
                        "label_for_charts": "Null",
                        "is_null_level": true
                    },
                    {
                        "sql_condition": "\"dob_l\" = \"dob_r\"",
                        "label_for_charts": "Exact match"
                    },
                    {
                        "sql_condition": "damerau_levenshtein(\"dob_l\", \"dob_r\") <= 1",
                        "label_for_charts": "Damerau_levenshtein <= 1"
                    },
                    {
                        "sql_condition": "\n            abs(date_diff('month',strptime(\"dob_l\",\n              '%Y-%m-%d'),strptime(\"dob_r\",\n              '%Y-%m-%d'))) <= 1\n        ",
                        "label_for_charts": "Within 1 month"
                    },
                    {
                        "sql_condition": "\n            abs(date_diff('year',strptime(\"dob_l\",\n              '%Y-%m-%d'),strptime(\"dob_r\",\n              '%Y-%m-%d'))) <= 1\n        ",
                        "label_for_charts": "Within 1 year"
                    },
                    {
                        "sql_condition": "\n            abs(date_diff('year',strptime(\"dob_l\",\n              '%Y-%m-%d'),strptime(\"dob_r\",\n              '%Y-%m-%d'))) <= 10\n        ",
                        "label_for_charts": "Within 10 years"
                    },
                    {
                        "sql_condition": "ELSE",
                        "label_for_charts": "All other comparisons"
                    }
                ],
                "comparison_description": "Exact match vs. Dob within damerau-levenshtein threshold 1 vs. Dates within the following thresholds Month(s): 1, Year(s): 1, Year(s): 10 vs. anything else"
            },
            {
                "output_column_name": "city",
                "comparison_levels": [
                    {
                        "sql_condition": "\"city_l\" IS NULL OR \"city_r\" IS NULL",
                        "label_for_charts": "Null",
                        "is_null_level": true
                    },
                    {
                        "sql_condition": "\"city_l\" = \"city_r\"",
                        "label_for_charts": "Exact match",
                        "tf_adjustment_column": "city",
                        "tf_adjustment_weight": 1.0
                    },
                    {
                        "sql_condition": "ELSE",
                        "label_for_charts": "All other comparisons"
                    }
                ],
                "comparison_description": "Exact match vs. anything else"
            },
            {
                "output_column_name": "email",
                "comparison_levels": [
                    {
                        "sql_condition": "\"email_l\" IS NULL OR \"email_r\" IS NULL",
                        "label_for_charts": "Null",
                        "is_null_level": true
                    },
                    {
                        "sql_condition": "\"email_l\" = \"email_r\"",
                        "label_for_charts": "Exact match"
                    },
                    {
                        "sql_condition": "levenshtein(\"email_l\", \"email_r\") <= 2",
                        "label_for_charts": "Levenshtein <= 2"
                    },
                    {
                        "sql_condition": "ELSE",
                        "label_for_charts": "All other comparisons"
                    }
                ],
                "comparison_description": "Exact match vs. Email within levenshtein threshold 2 vs. anything else"
            }
        ],
        "sql_dialect": "duckdb",
        "linker_uid": "wpYkgjrm",
        "probability_two_random_records_match": 0.0001
    }

    ```

With our finalised settings object, we can train a splink model using the following code:

??? example "Example model using the settings dictionary"

    ```py
    from splink.duckdb.linker import DuckDBLinker
    import splink.duckdb.comparison_library as cl
    import splink.duckdb.comparison_template_library as ctl

    import pandas as pd

    df = pd.read_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    settings = {
        "link_type": "dedupe_only",
        "blocking_rules_to_generate_predictions": [
            "l.first_name = r.first_name",
            "l.surname = r.surname",
        ],
        "comparisons": [
            ctl.name_comparison("first_name"),
            ctl.name_comparison("surname"),
            ctl.date_comparison("dob", cast_strings_to_date=True),
            cl.exact_match("city", term_frequency_adjustments=True),
            cl.levenshtein_at_thresholds("email", 2),
        ],
    }

    linker = DuckDBLinker(df, settings)
    linker.estimate_u_using_random_sampling(max_pairs=1e6)

    blocking_rule_for_training = "l.first_name = r.first_name and l.surname = r.surname"
    linker.estimate_parameters_using_expectation_maximisation(blocking_rule_for_training)

    blocking_rule_for_training = "l.dob = r.dob"
    linker.estimate_parameters_using_expectation_maximisation(blocking_rule_for_training)

    pairwise_predictions = linker.predict()

    clusters = linker.cluster_pairwise_predictions_at_threshold(pairwise_predictions, 0.95)
    clusters.as_pandas_dataframe(limit=5)
    ```



## Advanced usage of the settings dictionary

The section above refers to the three key aspects of the Splink settings dictionary. In reality, these are a small proportion of the possible parameters that can be defined within the settings. However, these additional parameters are used much less frequently, either because they are not required or they have a sensible default.

For a list of all possible parameters that can be used within the settings dictionary, see the [Settings Dictionary Reference](../settings_dict_guide.md) and the [Interactive Settings Editor](../settingseditor/editor.md).

## Saving a trained model

Once you have have a trained Splink model, it is often helpful to save out the model. The `save_model_to_json` function allows the user to save out the specifications of their trained model.

```py
linker.save_model_to_json("model.json")
```

which, using the example settings and model training from above, gives the following output:

??? note "Model JSON"

    ```json linenums="1"
    {
        "link_type": "dedupe_only",
        "blocking_rules_to_generate_predictions": [
            "l.first_name = r.first_name",
            "l.surname = r.surname"
        ],
        "comparisons": [
            {
                "output_column_name": "first_name",
                "comparison_levels": [
                    {
                        "sql_condition": "\"first_name_l\" IS NULL OR \"first_name_r\" IS NULL",
                        "label_for_charts": "Null",
                        "is_null_level": true
                    },
                    {
                        "sql_condition": "\"first_name_l\" = \"first_name_r\"",
                        "label_for_charts": "Exact match first_name",
                        "m_probability": 0.42539626585084356,
                        "u_probability": 0.0055691509084167595
                    },
                    {
                        "sql_condition": "damerau_levenshtein(\"first_name_l\", \"first_name_r\") <= 1",
                        "label_for_charts": "Damerau_levenshtein <= 1",
                        "m_probability": 0.07083503518755285,
                        "u_probability": 0.0015597577555308368
                    },
                    {
                        "sql_condition": "jaro_winkler_similarity(\"first_name_l\", \"first_name_r\") >= 0.9",
                        "label_for_charts": "Jaro_winkler_similarity >= 0.9",
                        "m_probability": 0.059652193684047713,
                        "u_probability": 0.0013298726980595723
                    },
                    {
                        "sql_condition": "jaro_winkler_similarity(\"first_name_l\", \"first_name_r\") >= 0.8",
                        "label_for_charts": "Jaro_winkler_similarity >= 0.8",
                        "m_probability": 0.07293119026085258,
                        "u_probability": 0.0060561117290816955
                    },
                    {
                        "sql_condition": "ELSE",
                        "label_for_charts": "All other comparisons",
                        "m_probability": 0.3711853150167032,
                        "u_probability": 0.9854851069089111
                    }
                ],
                "comparison_description": "Exact match vs. First_Name within levenshtein threshold 1 vs. First_Name within damerau-levenshtein threshold 1 vs. First_Name within jaro_winkler thresholds 0.9, 0.8 vs. anything else"
            },
            {
                "output_column_name": "surname",
                "comparison_levels": [
                    {
                        "sql_condition": "\"surname_l\" IS NULL OR \"surname_r\" IS NULL",
                        "label_for_charts": "Null",
                        "is_null_level": true
                    },
                    {
                        "sql_condition": "\"surname_l\" = \"surname_r\"",
                        "label_for_charts": "Exact match surname",
                        "m_probability": 0.4799599325681923,
                        "u_probability": 0.0074429557488431336
                    },
                    {
                        "sql_condition": "damerau_levenshtein(\"surname_l\", \"surname_r\") <= 1",
                        "label_for_charts": "Damerau_levenshtein <= 1",
                        "m_probability": 0.07008569271116605,
                        "u_probability": 0.0019083296710011445
                    },
                    {
                        "sql_condition": "jaro_winkler_similarity(\"surname_l\", \"surname_r\") >= 0.9",
                        "label_for_charts": "Jaro_winkler_similarity >= 0.9",
                        "m_probability": 0.020227438482587002,
                        "u_probability": 0.0006063412008846001
                    },
                    {
                        "sql_condition": "jaro_winkler_similarity(\"surname_l\", \"surname_r\") >= 0.8",
                        "label_for_charts": "Jaro_winkler_similarity >= 0.8",
                        "m_probability": 0.04851972996678833,
                        "u_probability": 0.003760255509361861
                    },
                    {
                        "sql_condition": "ELSE",
                        "label_for_charts": "All other comparisons",
                        "m_probability": 0.38120720627126636,
                        "u_probability": 0.9862821178699093
                    }
                ],
                "comparison_description": "Exact match vs. Surname within levenshtein threshold 1 vs. Surname within damerau-levenshtein threshold 1 vs. Surname within jaro_winkler thresholds 0.9, 0.8 vs. anything else"
            },
            {
                "output_column_name": "dob",
                "comparison_levels": [
                    {
                        "sql_condition": "\"dob_l\" IS NULL OR \"dob_r\" IS NULL",
                        "label_for_charts": "Null",
                        "is_null_level": true
                    },
                    {
                        "sql_condition": "\"dob_l\" = \"dob_r\"",
                        "label_for_charts": "Exact match",
                        "m_probability": 0.6110365914628999,
                        "u_probability": 0.003703703703703704
                    },
                    {
                        "sql_condition": "damerau_levenshtein(\"dob_l\", \"dob_r\") <= 1",
                        "label_for_charts": "Damerau_levenshtein <= 1",
                        "m_probability": 0.011899382842926385,
                        "u_probability": 0.0014194194194194194
                    },
                    {
                        "sql_condition": "\n            abs(date_diff('month',strptime(\"dob_l\",\n              '%Y-%m-%d'),strptime(\"dob_r\",\n              '%Y-%m-%d'))) <= 1\n        ",
                        "label_for_charts": "Within 1 month",
                        "m_probability": 0.14328591788922354,
                        "u_probability": 0.004904904904904905
                    },
                    {
                        "sql_condition": "\n            abs(date_diff('year',strptime(\"dob_l\",\n              '%Y-%m-%d'),strptime(\"dob_r\",\n              '%Y-%m-%d'))) <= 1\n        ",
                        "label_for_charts": "Within 1 year",
                        "m_probability": 0.23377741936500165,
                        "u_probability": 0.05535935935935936
                    },
                    {
                        "sql_condition": "\n            abs(date_diff('year',strptime(\"dob_l\",\n              '%Y-%m-%d'),strptime(\"dob_r\",\n              '%Y-%m-%d'))) <= 10\n        ",
                        "label_for_charts": "Within 10 years",
                        "u_probability": 0.3138258258258258
                    },
                    {
                        "sql_condition": "ELSE",
                        "label_for_charts": "All other comparisons",
                        "m_probability": 6.88439948607414e-07,
                        "u_probability": 0.6207867867867868
                    }
                ],
                "comparison_description": "Exact match vs. Dob within damerau-levenshtein threshold 1 vs. Dates within the following thresholds Month(s): 1, Year(s): 1, Year(s): 10 vs. anything else"
            },
            {
                "output_column_name": "city",
                "comparison_levels": [
                    {
                        "sql_condition": "\"city_l\" IS NULL OR \"city_r\" IS NULL",
                        "label_for_charts": "Null",
                        "is_null_level": true
                    },
                    {
                        "sql_condition": "\"city_l\" = \"city_r\"",
                        "label_for_charts": "Exact match",
                        "m_probability": 0.6594293995874598,
                        "u_probability": 0.09307809682341518,
                        "tf_adjustment_column": "city",
                        "tf_adjustment_weight": 1.0
                    },
                    {
                        "sql_condition": "ELSE",
                        "label_for_charts": "All other comparisons",
                        "m_probability": 0.34057060041254017,
                        "u_probability": 0.9069219031765848
                    }
                ],
                "comparison_description": "Exact match vs. anything else"
            },
            {
                "output_column_name": "email",
                "comparison_levels": [
                    {
                        "sql_condition": "\"email_l\" IS NULL OR \"email_r\" IS NULL",
                        "label_for_charts": "Null",
                        "is_null_level": true
                    },
                    {
                        "sql_condition": "\"email_l\" = \"email_r\"",
                        "label_for_charts": "Exact match",
                        "m_probability": 0.676510796673033,
                        "u_probability": 0.004070304803112018
                    },
                    {
                        "sql_condition": "levenshtein(\"email_l\", \"email_r\") <= 2",
                        "label_for_charts": "Levenshtein <= 2",
                        "m_probability": 0.07663549181271126,
                        "u_probability": 0.00038595529012665426
                    },
                    {
                        "sql_condition": "ELSE",
                        "label_for_charts": "All other comparisons",
                        "m_probability": 0.24685371151425578,
                        "u_probability": 0.9955437399067614
                    }
                ],
                "comparison_description": "Exact match vs. Email within levenshtein threshold 2 vs. anything else"
            }
        ],
        "sql_dialect": "duckdb",
        "linker_uid": "mHRb2HT7",
        "probability_two_random_records_match": 0.0001
    }
    ```

This is simply the settings dictionary with additional entries for `"m_probability"` and `"u_probability"` in each of the `"comparison_levels"`, which have estimated during model training.

For example in the first name exact match level:

```json linenums="16", hl_lines="4 5"
{
    "sql_condition": "\"first_name_l\" = \"first_name_r\"",
    "label_for_charts": "Exact match first_name",
    "m_probability": 0.42539626585084356,
    "u_probability": 0.0055691509084167595
},

```

where the `m_probability` and `u_probability` values here are then used to generate the match weight for an exact match on `"first_name"` between two records (i.e. the amount of evidence provided by records having the same first name) in model predictions.

## Loading a pre-trained model

When using a pre-trained model, you can read in the model from a json and recreate the linker object to make new pariwise predictions. For example:

```py
linker = DuckDBLinker(new_df)
linker.load_model("model.json")
```

Where the linker is initialised with a dataset, but no settings dictionary. Then the `load_model` function is used to add the settings dictionary (including the trained `"m_probability"` and `"u_probability"` values) to the linker.

Once you have loaded the model, you can generate predictions, clusters etc. as normal. For example:

```py
pairwise_predictions = linker.predict()
```