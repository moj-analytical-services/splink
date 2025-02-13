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
2. What **pairs of records** to consider (defined by [blocking rules](../blocking/blocking_rules.md))
3. What **features** to consider, and how they should be **compared** (defined by [comparisons](../comparisons/customising_comparisons.ipynb))


## Defining a Splink model with a settings dictionary

All aspects of a Splink model are defined via the `SettingsCreator` object.

For example, consider a simple model:

```py linenums="1"
import splink.comparison_library as cl
import splink.comparison_template_library as ctl

settings = SettingsCreator(
    link_type="dedupe_only",
    blocking_rules_to_generate_predictions=[
        block_on("first_name"),
        block_on("surname", "dob"),
    ],
    comparisons=[
        ctl.NameComparison("first_name"),
        ctl.NameComparison("surname"),
        ctl.DateComparison(
            "dob",
            input_is_string=True,
            datetime_metrics=["month", "year"],
            datetime_thresholds=[
                1,
                1,
            ],
        ),
        cl.ExactMatch("city").configure(term_frequency_adjustments=True),
        ctl.EmailComparison("email"),
    ],
)
```

Where:

**1. Type of linkage**

The `"link_type"` is defined as a deduplication for a single dataset.

```py linenums="5"
    link_type="dedupe_only",
```

**2. Pairs of records to consider**

The `"blocking_rules_to_generate_predictions"` define a subset of pairs of records for the model to be considered when making predictions. In this case, where there is a match on:

- `first_name`
- OR (`surname` AND `dob`).

```py linenums="6"
    blocking_rules_to_generate_predictions=[
            block_on("first_name"),
            block_on("surname", "dob"),
        ],
```

For more information on how blocking is used in Splink, see the [dedicated topic guide](../blocking/blocking_rules.md).

**3. Features to consider, and how they should be compared**

The `"comparisons"` define the features to be compared between records: `"first_name"`, `"surname"`, `"dob"`, `"city"` and `"email"`.

```py linenums="10"
    comparisons=[
        cl.NameComparison("first_name"),
        cl.NameComparison("surname"),
        cl.DateComparison(
            "dob",
            input_is_string=True,
            datetime_metrics=["month", "year"],
            datetime_thresholds=[
                1,
                1,
            ],
        ),
        cl.ExactMatch("city").configure(term_frequency_adjustments=True),
        cl.EmailComparison("email"),
    ],
```

Using functions from the [comparison library](../comparisons/customising_comparisons.ipynb#method-1-using-the-comparisonlibrary) to define **how** these features should be compared.



For more information on how comparisons are defined, see the [dedicated topic guide](../comparisons/customising_comparisons.ipynb).






With our finalised settings object, we can train a Splink model using the following code:

??? example "Example model using the settings dictionary"

    ```py
    import splink.comparison_library as cl
    import splink.comparison_template_library as ctl
    from splink import DuckDBAPI, Linker, SettingsCreator, block_on, splink_datasets

    db_api = DuckDBAPI()
    df = splink_datasets.fake_1000

    settings = SettingsCreator(
        link_type="dedupe_only",
        blocking_rules_to_generate_predictions=[
            block_on("first_name"),
            block_on("surname"),
        ],
        comparisons=[
            ctl.NameComparison("first_name"),
            ctl.NameComparison("surname"),
            ctl.DateComparison(
                "dob",
                input_is_string=True,
                datetime_metrics=["month", "year"],
                datetime_thresholds=[
                    1,
                    1,
                ],
            ),
            cl.ExactMatch("city").configure(term_frequency_adjustments=True),
            ctl.EmailComparison("email"),
        ],
    )

    linker = Linker(df, settings, db_api=db_api)
    linker.training.estimate_u_using_random_sampling(max_pairs=1e6)

    blocking_rule_for_training = block_on("first_name", "surname")
    linker.training.estimate_parameters_using_expectation_maximisation(blocking_rule_for_training)

    blocking_rule_for_training = block_on("dob")
    linker.training.estimate_parameters_using_expectation_maximisation(blocking_rule_for_training)

    pairwise_predictions = linker.inference.predict()

    clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(pairwise_predictions, 0.95)
    clusters.as_pandas_dataframe(limit=5)

    ```



## Advanced usage of the settings dictionary

The section above refers to the three key aspects of the Splink settings dictionary. There are a variety of other lesser used settings, which can be found as the arguments to the `SettingsCreator`



## Saving a trained model

Once you have have a trained Splink model, it is often helpful to save out the model. The `save_model_to_json` function allows the user to save out the specifications of their trained model.

```py
linker.misc.save_model_to_json("model.json")
```

which, using the example settings and model training from above, gives the following output:

??? note "Model JSON"

    When the splink model is saved to disk using `linker.misc.save_model_to_json("model.json")` these settings become:


    ```json
    {
        "link_type": "dedupe_only",
        "probability_two_random_records_match": 0.0008208208208208208,
        "retain_matching_columns": true,
        "retain_intermediate_calculation_columns": false,
        "additional_columns_to_retain": [],
        "sql_dialect": "duckdb",
        "linker_uid": "29phy7op",
        "em_convergence": 0.0001,
        "max_iterations": 25,
        "bayes_factor_column_prefix": "bf_",
        "term_frequency_adjustment_column_prefix": "tf_",
        "comparison_vector_value_column_prefix": "gamma_",
        "unique_id_column_name": "unique_id",
        "source_dataset_column_name": "source_dataset",
        "blocking_rules_to_generate_predictions": [
            {
                "blocking_rule": "l.\"first_name\" = r.\"first_name\"",
                "sql_dialect": "duckdb"
            },
            {
                "blocking_rule": "l.\"surname\" = r.\"surname\"",
                "sql_dialect": "duckdb"
            }
        ],
        "comparisons": [
            {
                "output_column_name": "first_name",
                "comparison_levels": [
                    {
                        "sql_condition": "\"first_name_l\" IS NULL OR \"first_name_r\" IS NULL",
                        "label_for_charts": "first_name is NULL",
                        "is_null_level": true
                    },
                    {
                        "sql_condition": "\"first_name_l\" = \"first_name_r\"",
                        "label_for_charts": "Exact match on first_name",
                        "m_probability": 0.48854806009621365,
                        "u_probability": 0.0056770619302010565
                    },
                    {
                        "sql_condition": "jaro_winkler_similarity(\"first_name_l\", \"first_name_r\") >= 0.9",
                        "label_for_charts": "Jaro-Winkler distance of first_name >= 0.9",
                        "m_probability": 0.1903763096120358,
                        "u_probability": 0.003424501164330396
                    },
                    {
                        "sql_condition": "jaro_winkler_similarity(\"first_name_l\", \"first_name_r\") >= 0.8",
                        "label_for_charts": "Jaro-Winkler distance of first_name >= 0.8",
                        "m_probability": 0.08609678978546921,
                        "u_probability": 0.006620702251038765
                    },
                    {
                        "sql_condition": "ELSE",
                        "label_for_charts": "All other comparisons",
                        "m_probability": 0.23497884050628137,
                        "u_probability": 0.9842777346544298
                    }
                ],
                "comparison_description": "jaro_winkler at thresholds 0.9, 0.8 vs. anything else"
            },
            {
                "output_column_name": "surname",
                "comparison_levels": [
                    {
                        "sql_condition": "\"surname_l\" IS NULL OR \"surname_r\" IS NULL",
                        "label_for_charts": "surname is NULL",
                        "is_null_level": true
                    },
                    {
                        "sql_condition": "\"surname_l\" = \"surname_r\"",
                        "label_for_charts": "Exact match on surname",
                        "m_probability": 0.43210610613512185,
                        "u_probability": 0.004322481469643699
                    },
                    {
                        "sql_condition": "jaro_winkler_similarity(\"surname_l\", \"surname_r\") >= 0.9",
                        "label_for_charts": "Jaro-Winkler distance of surname >= 0.9",
                        "m_probability": 0.2514700606335103,
                        "u_probability": 0.002907020988387136
                    },
                    {
                        "sql_condition": "jaro_winkler_similarity(\"surname_l\", \"surname_r\") >= 0.8",
                        "label_for_charts": "Jaro-Winkler distance of surname >= 0.8",
                        "m_probability": 0.0757748206402343,
                        "u_probability": 0.0033636211436311888
                    },
                    {
                        "sql_condition": "ELSE",
                        "label_for_charts": "All other comparisons",
                        "m_probability": 0.2406490125911336,
                        "u_probability": 0.989406876398338
                    }
                ],
                "comparison_description": "jaro_winkler at thresholds 0.9, 0.8 vs. anything else"
            },
            {
                "output_column_name": "dob",
                "comparison_levels": [
                    {
                        "sql_condition": "\"dob_l\" IS NULL OR \"dob_r\" IS NULL",
                        "label_for_charts": "dob is NULL",
                        "is_null_level": true
                    },
                    {
                        "sql_condition": "\"dob_l\" = \"dob_r\"",
                        "label_for_charts": "Exact match on dob",
                        "m_probability": 0.39025358731716286,
                        "u_probability": 0.0016036280808555408
                    },
                    {
                        "sql_condition": "damerau_levenshtein(\"dob_l\", \"dob_r\") <= 1",
                        "label_for_charts": "Damerau-Levenshtein distance of dob <= 1",
                        "m_probability": 0.1489444378965258,
                        "u_probability": 0.0016546990388445707
                    },
                    {
                        "sql_condition": "ABS(EPOCH(try_strptime(\"dob_l\", '%Y-%m-%d')) - EPOCH(try_strptime(\"dob_r\", '%Y-%m-%d'))) <= 2629800.0",
                        "label_for_charts": "Abs difference of 'transformed dob <= 1 month'",
                        "m_probability": 0.08866691175438302,
                        "u_probability": 0.002594404665842722
                    },
                    {
                        "sql_condition": "ABS(EPOCH(try_strptime(\"dob_l\", '%Y-%m-%d')) - EPOCH(try_strptime(\"dob_r\", '%Y-%m-%d'))) <= 31557600.0",
                        "label_for_charts": "Abs difference of 'transformed dob <= 1 year'",
                        "m_probability": 0.10518866178811104,
                        "u_probability": 0.030622146410222362
                    },
                    {
                        "sql_condition": "ELSE",
                        "label_for_charts": "All other comparisons",
                        "m_probability": 0.26694640124381713,
                        "u_probability": 0.9635251218042348
                    }
                ],
                "comparison_description": "Exact match vs. Damerau-Levenshtein distance <= 1 vs. month difference <= 1 vs. year difference <= 1 vs. anything else"
            },
            {
                "output_column_name": "city",
                "comparison_levels": [
                    {
                        "sql_condition": "\"city_l\" IS NULL OR \"city_r\" IS NULL",
                        "label_for_charts": "city is NULL",
                        "is_null_level": true
                    },
                    {
                        "sql_condition": "\"city_l\" = \"city_r\"",
                        "label_for_charts": "Exact match on city",
                        "m_probability": 0.561103053663773,
                        "u_probability": 0.052019405886043986,
                        "tf_adjustment_column": "city",
                        "tf_adjustment_weight": 1.0
                    },
                    {
                        "sql_condition": "ELSE",
                        "label_for_charts": "All other comparisons",
                        "m_probability": 0.438896946336227,
                        "u_probability": 0.947980594113956
                    }
                ],
                "comparison_description": "Exact match 'city' vs. anything else"
            },
            {
                "output_column_name": "email",
                "comparison_levels": [
                    {
                        "sql_condition": "\"email_l\" IS NULL OR \"email_r\" IS NULL",
                        "label_for_charts": "email is NULL",
                        "is_null_level": true
                    },
                    {
                        "sql_condition": "\"email_l\" = \"email_r\"",
                        "label_for_charts": "Exact match on email",
                        "m_probability": 0.5521904988218763,
                        "u_probability": 0.0023577568563241916
                    },
                    {
                        "sql_condition": "NULLIF(regexp_extract(\"email_l\", '^[^@]+', 0), '') = NULLIF(regexp_extract(\"email_r\", '^[^@]+', 0), '')",
                        "label_for_charts": "Exact match on transformed email",
                        "m_probability": 0.22046667643566936,
                        "u_probability": 0.0010970118706508391
                    },
                    {
                        "sql_condition": "jaro_winkler_similarity(\"email_l\", \"email_r\") >= 0.88",
                        "label_for_charts": "Jaro-Winkler distance of email >= 0.88",
                        "m_probability": 0.21374764835824084,
                        "u_probability": 0.0007367990176013098
                    },
                    {
                        "sql_condition": "jaro_winkler_similarity(NULLIF(regexp_extract(\"email_l\", '^[^@]+', 0), ''), NULLIF(regexp_extract(\"email_r\", '^[^@]+', 0), '')) >= 0.88",
                        "label_for_charts": "Jaro-Winkler distance of transformed email >= 0.88",
                        "u_probability": 0.00027834629553827263
                    },
                    {
                        "sql_condition": "ELSE",
                        "label_for_charts": "All other comparisons",
                        "m_probability": 0.013595176384213488,
                        "u_probability": 0.9955300859598853
                    }
                ],
                "comparison_description": "jaro_winkler on username at threshold 0.88 vs. anything else"
            }
        ]
    }

    ```


This is simply the settings dictionary with additional entries for `"m_probability"` and `"u_probability"` in each of the `"comparison_levels"`, which have estimated during model training.

For example in the first name exact match level:

```json linenums="16", hl_lines="4 5"
{
    "sql_condition": "\"first_name_l\" = \"first_name_r\"",
    "label_for_charts": "Exact match on first_name",
    "m_probability": 0.48854806009621365,
    "u_probability": 0.0056770619302010565
},

```

where the `m_probability` and `u_probability` values here are then used to generate the match weight for an exact match on `"first_name"` between two records (i.e. the amount of evidence provided by records having the same first name) in model predictions.

## Loading a pre-trained model

When using a pre-trained model, you can read in the model from a json and recreate the linker object to make new pairwise predictions. For example:

```py
linker = Linker(
    new_df,
    settings="./path/to/model.json",
    db_api=db_api
)

```

