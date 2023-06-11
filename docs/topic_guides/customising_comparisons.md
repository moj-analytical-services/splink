# Defining and customising how record comparisons are made

A key feature of Splink is the ability to customise how record comparisons are made - that is, how similarity is defined for different data types.  For example, the definition of similarity that is appropriate for a date of birth field is different than for a first name field.

By tailoring the definitions of similarity, linking models are more effectively able to distinguish beteween different gradations of similarity, leading to more accurate data linking models.

Note that for performance reasons, Splink requires the user to define `n` discrete levels (gradations) of similarity.

## Comparing information

Comparisons are defined on pairwise record comparisons.  Suppose for instance your data contains `first_name` and `surname` and `dob`:

|id |first_name|surname|dob       |
|---|----------|-------|----------|
|1  |john      |smith  |1991-04-11|
|2  |jon       |smith  |1991-04-17|
|3  |john      |smyth  |1991-04-11|

To compare these records, at the blocking stage, Splink will set these records against each other in a table of pairwise record comparisons:

|id_l|id_r|first_name_l|first_name_r|surname_l|surname_r|dob_l     |dob_r     |
|----|----|------------|------------|---------|---------|----------|----------|
|1   |2   |john        |jon         |smith    |smith    |1991-04-11|1991-04-17|
|1   |3   |john        |john        |smith    |smyth    |1991-04-11|1991-04-11|
|2   |3   |jon         |john        |smith    |smyth    |1991-04-17|1991-04-11|


When defining comparisons, we are defining rules that operate on each row of this latter table of pairwise comparisons

<hr>

## `Comparisons`, `ComparisonTemplates` and `ComparisonLevels`

A Splink model contains a collection of `Comparisons` and `ComparisonLevels` organised in a hierarchy.  An example is as follows:

```
Data Linking Model
├─-- Comparison: Date of birth
│    ├─-- ComparisonLevel: Exact match
│    ├─-- ComparisonLevel: Up to one character difference
│    ├─-- ComparisonLevel: Up to three character difference
│    ├─-- ComparisonLevel: All other
├─-- Comparison: Name
│    ├─-- ComparisonLevel: Exact match on first name and surname
│    ├─-- ComparisonLevel: Exact match on first name
│    ├─-- etc.
```

A fuller description of `Comaprison`s and `ComparisonLevel`s can be found [here](https://moj-analytical-services.github.io/splink/comparison.html) and [here](https://moj-analytical-services.github.io/splink/comparison_level.html) respectively.


How are these comparisons specified?

### Three ways of specifying Comparisons

In Splink, there are three ways of specifying `Comparisons`:

- Using pre-baked comparisons from a backend's `ComparisonLibrary` or `ComparisonTemplateLibrary`.   (Most simple/succinct)
- Composing pre-defined `ComparisonLevels` from a backend's `ComparisonLevelLibrary`
- Writing a full spec of a `Comparison` by hand (most verbose/flexible)

<hr>

#### Method 1: Using the `ComparisonLibrary`

The `ComparisonLibrary` for a each backend (`DuckDB`, `Spark`, etc.) contains pre-baked similarity functions that cover many common use cases.

These functions generate an entire `Comparison`, composed of several `ComparisonLevels`

The following provides an example of using the `ComparisonLibrary` for DuckDB.

```py
import splink.duckdb.duckdb_comparison_library as cl

first_name_comparison = cl.exact_match("first_name")
print(first_name_comparison.human_readable_description)
```
```
Comparison 'Exact match vs. anything else' of "first_name".
Similarity is assessed using the following ComparisonLevels:
    - 'Null' with SQL rule: "first_name_l" IS NULL OR "first_name_r" IS NULL
    - 'Exact match' with SQL rule: "first_name_l" = "first_name_r"
    - 'All other comparisons' with SQL rule: ELSE
```

Note that, under the hood, these functions generate a Python dictionary, which conforms to the underlying `.json` specification of a model:

```py
first_name_comparison.as_dict()
```

```py
{'output_column_name': 'first_name',
'comparison_levels': [
    {
        'sql_condition': '"first_name_l" IS NULL OR "first_name_r" IS NULL',
        'label_for_charts': 'Null',
        'is_null_level': True
    },
    {
        'sql_condition': '"first_name_l" = "first_name_r"',
        'label_for_charts': 'Exact match'
    },
    {
        'sql_condition': 'ELSE', 
        'label_for_charts': 'All other comparisons'
    }],
'comparison_description': 'Exact match vs. anything else'}
```


#### Method 2: Using the `ComparisonTemplateLibrary`

The `ComparisonTemplateLibrary` is very similar to `ComparisonLibrary` in that it contains pre-baked similarity functions for each backend (DuckDB, Spark, etc.) to cover common use cases.

The key difference is that `ComparisonTemplateLibrary` contains functions to generate a 'best practice' `Comparison` based on the type of data in a given column. This includes: 

- How comparison is structured (what comparison levels are included, and in what order) 
- Default parameters (e.g. `damerau_levenshtein_thresholds = [1]`)

The following provides an example of using the ComparisonTemplateLibrary for DuckDB.

```py
import splink.duckdb.duckdb_comparison_template_library as ctl

date_of_birth_comparison = ctl.date_comparison("date_of_birth")
print(date_of_birth_comparison.human_readable_description)
```
```
Comparison 'Exact match vs. Date_Of_Birth within damerau-levenshtein threshold 1 vs. Dates within the following thresholds Month(s): 1, Year(s): 1, Year(s): 10 vs. anything else' of "date_of_birth".
Similarity is assessed using the following ComparisonLevels:
    - 'Null' with SQL rule: "date_of_birth_l" IS NULL OR "date_of_birth_r" IS NULL
    - 'Exact match' with SQL rule: "date_of_birth_l" = "date_of_birth_r"
    - 'Damerau_levenshtein <= 1' with SQL rule: damerau_levenshtein("date_of_birth_l", "date_of_birth_r") <= 1
    - 'Within 1 month' with SQL rule: 
            abs(date_diff('month', "date_of_birth_l",
            "date_of_birth_r")) <= 1
        
    - 'Within 1 year' with SQL rule: 
            abs(date_diff('year', "date_of_birth_l",
            "date_of_birth_r")) <= 1
        
    - 'Within 10 years' with SQL rule: 
            abs(date_diff('year', "date_of_birth_l",
            "date_of_birth_r")) <= 10
        
    - 'All other comparisons' with SQL rule: ELSE
```

These `Comparisons` can be specified in a data linking model as follows:

```py
settings = {
    "link_type": "dedupe_only",
    "blocking_rules_to_generate_predictions": [
        "l.first_name = r.first_name",
        "l.surname = r.surname",
    ],
    "comparisons": [
        exact_match("first_name"),
        date_comparison("dob"),
    ],
}
```

You can customise a `ComparisonTemplate` by choosing your own values for the [function parameters](../comparison_template_library.md), but for anything more bespoke you will want to construct a `Comparison` with `ComparisonLevels` or provide the spec as a dictionary.

For a deep dive on Comparison Templates, see the dedicated [topic guide](comparison_templates.ipynb).


#### Method 3: `ComparisonLevels`

The `ComparisonLevels` API provides a lower-level API that gives the user greater control over their comparisons.

For example, the user may wish to specify a comparison that has levels for a match on dmetaphone and jaro_winkler of the `first_name` field.  

The below example assumes the user has derived a column `dmeta_first_name` which contains the dmetaphone of the first name.

```py
import splink.spark.spark_comparison_level_library as cll

comparison_first_name = {
    "output_column_name": "first_name",
    "comparison_description": "First name jaro dmeta",
    "comparison_levels": [
        cll.null_level("first_name"),
        cll.exact_match_level("first_name", term_frequency_adjustments=True),
        cll.exact_match_level("dmeta_first_name", term_frequency_adjustments=True),
        cll.else_level(),
    ],
}

print(comparison_first_name.human_readable_description)
```
```
Comparison 'First name jaro dmeta' of `first_name` and `dmeta_first_name`.
Similarity is assessed using the following ComparisonLevels:
    - 'Null' with SQL rule: `first_name_l` IS NULL OR `first_name_r` IS NULL
    - 'Exact match' with SQL rule: `first_name_l` = `first_name_r`
    - 'Exact match' with SQL rule: `dmeta_first_name_l` = `dmeta_first_name_r`
    - 'All other comparisons' with SQL rule: ELSE
```

This can now be specified in the settings dictionary as follows:

```py
import splink.spark.spark_comparison_library as cl

settings = {
    "link_type": "dedupe_only",
    "blocking_rules_to_generate_predictions": [
        "l.first_name = r.first_name",
        "l.surname = r.surname",
    ],
    "comparisons": [
        comparison_first_name,  # The comparison specified above using ComparisonLevels
        cl.levenshtein_at_thresholds(
            "dob", [1, 2], term_frequency_adjustments=True
        ),  # From comparison_library
    ],
}
```


#### Method 4: Providing the spec as a dictionary

Ultimately, comparisons are specified as a dictionary which conforms to [the formal `jsonschema` specification of the settings dictionary](https://github.com/moj-analytical-services/splink/blob/master/splink/files/settings_jsonschema.json) and [here](https://moj-analytical-services.github.io/splink/).

The library functions described above are convenience functions that provide a shorthand way to produce valid dictionaries.

For maximium control over your settings, you can specify your comparisons as a dictionary.

```py
comparison_first_name = {
    "output_column_name": "first_name",
    "comparison_description": "First name jaro dmeta",
    "comparison_levels": [
        {
            "sql_condition": "first_name_l IS NULL OR first_name_r IS NULL",
            "label_for_charts": "Null",
            "is_null_level": True,
        },
        {
            "sql_condition": "first_name_l = first_name_r",
            "label_for_charts": "Exact match",
            "tf_adjustment_column": "first_name",
            "tf_adjustment_weight": 1.0,
            "tf_minimum_u_value": 0.001,
        },
        {
            "sql_condition": "dmeta_first_name_l = dmeta_first_name_r",
            "label_for_charts": "Exact match",
            "tf_adjustment_column": "dmeta_first_name",
            "tf_adjustment_weight": 1.0,
        },
        {
            "sql_condition": "jaro_winkler_sim(first_name_l, first_name_r) > 0.8",
            "label_for_charts": "Exact match",
            "tf_adjustment_column": "first_name",
            "tf_adjustment_weight": 0.5,
            "tf_minimum_u_value": 0.001,
        },
        {"sql_condition": "ELSE", "label_for_charts": "All other comparisons"},
    ],
}

settings = {
    "link_type": "dedupe_only",
    "blocking_rules_to_generate_predictions": [
        "l.first_name = r.first_name",
        "l.surname = r.surname",
    ],
    "comparisons": [
        comparison_first_name,  # The comparison specified above using the dict
        cl.levenshtein_at_thresholds(
            "dob", [1, 2], term_frequency_adjustments=True
        ),  # From comparison_library
    ],
}
```

## Examples

Below are some examples of how you can define the same comparison, but through different methods.

Note: the following examples show working code for duckdb. In order to change to Where functions exist

### Exact match Comparison with Term-Frequency Adjustments

???+ example 

    ===+ "Comparison Library"

        ```py
        import splink.duckdb.duckdb_comparison_library as cl

        first_name_comparison = cl.exact_match("first_name", term_frequency_adjustments=True)
        ```

    === "Comparison Level Library"

        ```py
        import splink.duckdb.duckdb_comparison_level_library as cll

        first_name_comparison = {
            "output_column_name": "first_name",
            "comparison_description": "Exact match vs. anything else",
            "comparison_levels": [
                cll.null_level("first_name"),
                cll.exact_match_level("first_name", term_frequency_adjustments=True),
                cll.else_level(),
            ],
        }
        ```
        
    === "Settings Dictionary"

        ```py
        first_name_comparison = {
            'output_column_name': 'first_name',
            'comparison_levels': [
                {
                    'sql_condition': '"first_name_l" IS NULL OR "first_name_r" IS NULL',
                    'label_for_charts': 'Null',
                    'is_null_level': True
                },
                {
                    'sql_condition': '"first_name_l" = "first_name_r"',
                    'label_for_charts': 'Exact match',
                    'tf_adjustment_column': 'first_name',
                    'tf_adjustment_weight': 1.0
                },
                {
                    'sql_condition': 'ELSE', 
                    'label_for_charts': 'All other comparisons'
                }],
            'comparison_description': 'Exact match vs. anything else'
        }

        ```
    Each of which gives

    ```json
    {
        'output_column_name': 'first_name',
        'comparison_levels': [
            {
                'sql_condition': '"first_name_l" IS NULL OR "first_name_r" IS NULL',
                'label_for_charts': 'Null',
                'is_null_level': True
            },
            {
                'sql_condition': '"first_name_l" = "first_name_r"',
                'label_for_charts': 'Exact match',
                'tf_adjustment_column': 'first_name',
                'tf_adjustment_weight': 1.0
            },
            {
                'sql_condition': 'ELSE', 
                'label_for_charts': 'All other comparisons'
            }],
        'comparison_description': 'Exact match vs. anything else'
    }
    ```
    in your settings dictionary.

### Name Comparison

??? example

    ===+ "Comparison Template Library"

        ```py
        import splink.duckdb.duckdb_comparison_template_library as ctl

        surname_comparison = ctl.name_comparison("surname")
        ```

    === "Comparison Level Library"

        ```py
        import splink.duckdb.duckdb_comparison_level_library as cll

            surname_comparison = {
                "output_column_name": "surname",
                "comparison_description": "Exact match vs. Surname within jaro_winkler thresholds 0.95, 0.88 vs. anything else",
                "comparison_levels": [
                    cll.null_level("surname"),
                    cll.exact_match_level("surname"),
                    cll.damerau_levenshtein_level("surname", 1)
                    cll.jaro_winkler_level("surname", 0.9),
                    cll.jaro_winkler_level("surname", 0.8),
                    cll.else_level(),
                ],
            }
        ```

    === "Settings Dictionary"

        ```py
        surname_comparison = {
            'output_column_name': 'surname',
            'comparison_levels': [
                {
                    'sql_condition': '"surname_l" IS NULL OR "surname_r" IS NULL',
                    'label_for_charts': 'Null',
                    'is_null_level': True
                },
                {
                    'sql_condition': '"surname_l" = "surname_r"',
                    'label_for_charts': 'Exact match'
                },
                {
                    'sql_condition': 'damerau_levenshtein("surname_l", "surname_r") <= 1',
                    'label_for_charts': 'Damerau_levenshtein <= 1'
                },
                {
                    'sql_condition': 'jaro_winkler_similarity("surname_l", "surname_r") >= 0.9',
                    'label_for_charts': 'Jaro_winkler_similarity >= 0.9'
                },
                {
                    'sql_condition': 'jaro_winkler_similarity("surname_l", "surname_r") >= 0.8',
                    'label_for_charts': 'Jaro_winkler_similarity >= 0.8'
                },
                {
                    'sql_condition': 'ELSE', 'label_for_charts': 'All other comparisons'
                }],
                 'comparison_description': 'Exact match vs. Surname within levenshtein threshold 1 vs. Surname within damerau-levenshtein threshold 1 vs. Surname within jaro_winkler thresholds 0.9, 0.8 vs. anything else'
                }
        ```


    Each of which gives

    ```json
    {
            'output_column_name': 'surname',
            'comparison_levels': [
                {
                    'sql_condition': '"surname_l" IS NULL OR "surname_r" IS NULL',
                    'label_for_charts': 'Null',
                    'is_null_level': True
                },
                {
                    'sql_condition': '"surname_l" = "surname_r"',
                    'label_for_charts': 'Exact match'
                },
                {
                    'sql_condition': 'damerau_levenshtein("surname_l", "surname_r") <= 1',
                    'label_for_charts': 'Damerau_levenshtein <= 1'
                },
                {
                    'sql_condition': 'jaro_winkler_similarity("surname_l", "surname_r") >= 0.9',
                    'label_for_charts': 'Jaro_winkler_similarity >= 0.9'
                },
                {
                    'sql_condition': 'jaro_winkler_similarity("surname_l", "surname_r") >= 0.8',
                    'label_for_charts': 'Jaro_winkler_similarity >= 0.8'
                },
                {
                    'sql_condition': 'ELSE', 'label_for_charts': 'All other comparisons'
                }],
                 'comparison_description': 'Exact match vs. Surname within levenshtein threshold 1 vs. Surname within damerau-levenshtein threshold 1 vs. Surname within jaro_winkler thresholds 0.9, 0.8 vs. anything else'
    }
    ```
    in your settings dictionary.

### Levenshtein Comparison

??? example

    ===+ "Comparison Library"

        ```py
        import splink.duckdb.duckdb_comparison_library as cl

        email_comparison = cl.levenshtein_at_thresholds("email", [2, 4])
        ```

    === "Comparison Level Library"

        ```py
        import splink.duckdb.duckdb_comparison_level_library as cll

        email_comparison = {
            "output_column_name": "email",
            "comparison_description": "Exact match vs. Email within levenshtein thresholds 2, 4 vs. anything else",
            "comparison_levels": [
                cll.null_level("email"),
                cll.exact_match_level("surname"),
                cll.levenshtein_level("surname", 2),
                cll.levenshtein_level("surname", 4),
                cll.else_level(),
            ],
        }
        ```

    === "Settings Dictionary"

        ```py
        email_comparison = {
            'output_column_name': 'email',
            'comparison_levels': [{'sql_condition': '"email_l" IS NULL OR "email_r" IS NULL',
            'label_for_charts': 'Null',
            'is_null_level': True},
            {
                'sql_condition': '"email_l" = "email_r"',
                'label_for_charts': 'Exact match'
            },
            {
                'sql_condition': 'levenshtein("email_l", "email_r") <= 2',
                'label_for_charts': 'Levenshtein <= 2'
            },
            {
                'sql_condition': 'levenshtein("email_l", "email_r") <= 4',
                'label_for_charts': 'Levenshtein <= 4'
            },
            {
                'sql_condition': 'ELSE', 
                'label_for_charts': 'All other comparisons'
            }],
            'comparison_description': 'Exact match vs. Email within levenshtein thresholds 2, 4 vs. anything else'}
        ```

    Each of which gives

    ```json
    {
        'output_column_name': 'email',
        'comparison_levels': [
            {
                'sql_condition': '"email_l" IS NULL OR "email_r" IS NULL',
                'label_for_charts': 'Null',
                'is_null_level': True},
            {
                'sql_condition': '"email_l" = "email_r"',
                'label_for_charts': 'Exact match'
            },
            {
                'sql_condition': 'levenshtein("email_l", "email_r") <= 2',
                'label_for_charts': 'Levenshtein <= 2'
            },
            {
                'sql_condition': 'levenshtein("email_l", "email_r") <= 4',
                'label_for_charts': 'Levenshtein <= 4'
            },
            {
                'sql_condition': 'ELSE', 
                'label_for_charts': 'All other comparisons'
            }],
        'comparison_description': 'Exact match vs. Email within levenshtein thresholds 2, 4 vs. anything else'
    }
    ```

    in your settings dictionary.

### Date Comparison

??? example

    ===+ "Comparison Template Library"

        ```py
        import splink.duckdb.duckdb_comparison_template_library as ctl

        dob_comparison = ctl.date_comparison("date_of_birth")
        ```

    === "Comparison Level Library"

        ```py
        import splink.duckdb.duckdb_comparison_level_library as cll

        dob_comparison = {
                    "output_column_name": "date_of_birth",
                    "comparison_description": "Exact match vs. Date_Of_Birth within damerau-levenshtein thresholds 1 vs. Dates within the following thresholds Month(s): 1, Year(s): 1, Year(s): 10 vs. anything else",
                    "comparison_levels": [
                        cll.null_level("date_of_birth"),
                        cll.exact_match_level("date_of_birth"),
                        cll.damerau_levenshtein_level("date_of_birth", 1),
                        cll.datediff_level("date_of_birth",
                                            date_threshold=1,
                                            date_metric="month"),
                        cll.datediff_level("date_of_birth",
                                            date_threshold=1,
                                            date_metric="year"),
                        cll.datediff_level("date_of_birth",
                                            date_threshold=10,
                                            date_metric="year"),
                        cll.else_level(),
                    ],
                }
        ```

    === "Settings Dictionary"

        ```py
        dob_comparison = {
            'output_column_name': 'date_of_birth',
            'comparison_levels': [
                {
                    'sql_condition': '"date_of_birth_l" IS NULL OR "date_of_birth_r" IS NULL',
                    'label_for_charts': 'Null',
                    'is_null_level': True
                },
                {
                    'sql_condition': '"date_of_birth_l" = "date_of_birth_r"',
                    'label_for_charts': 'Exact match'
                },
                {
                    'sql_condition': 'damerau_levenshtein("date_of_birth_l", "date_of_birth_r") <= 1',
                'label_for_charts': 'Damerau_levenshtein <= 1'
                },
                {
                    'sql_condition': 'abs(date_diff(\'month\', "date_of_birth_l", "date_of_birth_r")) <= 1',
                    'label_for_charts': 'Within 1 month'
                },
                {
                    'sql_condition': 'abs(date_diff(\'year\', "date_of_birth_l", "date_of_birth_r")) <= 1',
                    'label_for_charts': 'Within 1 year'
                },
                {
                    'sql_condition': 'abs(date_diff(\'year\', "date_of_birth_l", "date_of_birth_r")) <= 10',
                    'label_for_charts': 'Within 10 years'},
                {
                    'sql_condition': 'ELSE', 'label_for_charts': 'All other comparisons'
                }],
            'comparison_description': 'Exact match vs. Date_Of_Birth within damerau-levenshtein threshold 1 vs. Dates within the following thresholds Month(s): 1, Year(s): 1, Year(s): 10 vs. anything else'
        }
        ```


    Each of which gives

    ```json
    {
            'output_column_name': 'date_of_birth',
            'comparison_levels': [
                {
                    'sql_condition': '"date_of_birth_l" IS NULL OR "date_of_birth_r" IS NULL',
                    'label_for_charts': 'Null',
                    'is_null_level': True
                },
                {
                    'sql_condition': '"date_of_birth_l" = "date_of_birth_r"',
                    'label_for_charts': 'Exact match'
                },
                {
                    'sql_condition': 'damerau_levenshtein("date_of_birth_l", "date_of_birth_r") <= 1',
                'label_for_charts': 'Damerau_levenshtein <= 1'
                },
                {
                    'sql_condition': 'abs(date_diff(\'month\', "date_of_birth_l", "date_of_birth_r")) <= 1',
                    'label_for_charts': 'Within 1 month'
                },
                {
                    'sql_condition': 'abs(date_diff(\'year\', "date_of_birth_l", "date_of_birth_r")) <= 1',
                    'label_for_charts': 'Within 1 year'
                },
                {
                    'sql_condition': 'abs(date_diff(\'year\', "date_of_birth_l", "date_of_birth_r")) <= 10',
                    'label_for_charts': 'Within 10 years'},
                {
                    'sql_condition': 'ELSE', 'label_for_charts': 'All other comparisons'
                }],
            'comparison_description': 'Exact match vs. Date_Of_Birth within damerau-levenshtein threshold 1 vs. Dates within the following thresholds Month(s): 1, Year(s): 1, Year(s): 10 vs. anything else'
    }
    ```
    in your settings dictionary.

### KM Distance between coordinates

??? example

    === "Comparison Library"

        ```py
        import splink.duckdb.duckdb_comparison_library as cl

        distance_comparison = cl.distance_in_km_at_thresholds("lat_col",
                                "long_col",
                                km_thresholds = [0.1, 1, 10]
                                )
        ```

    === "Comparison Level Library"

        ```py
        distance_comparison = {
                        "output_column_name": "custom_lat_col_long_col",
                        "comparison_description": "Km distance within the following thresholds Km threshold(s): 0.1, Km threshold(s): 1, Km threshold(s): 10 vs. anything else",
                        "comparison_levels": [
                            cll.or_(
                                cll.null_level("lat_col"),
                                cll.null_level("long_col"),
                                ),
                            cll.distance_in_km_level(
                                "lat_col",
                                "long_col",
                                km_threshold=0.1),
                            cll.distance_in_km_level(
                                "lat_col",
                                "long_col",
                                km_threshold=1),
                            cll.distance_in_km_level(
                                "lat_col",
                                "long_col",
                                km_threshold=10),
                            cll.else_level(),
                        ],
                    }
        ```

    === "Settings Dictionary"

        ```py
        distance_comparison = {
            'output_column_name': 'custom_lat_col_long_col',
            'comparison_levels': [
                {
                    'sql_condition': '(lat_col_l IS NULL OR lat_col_r IS NULL) \nOR (long_col_l IS NULL OR long_col_r IS NULL)',
                    'label_for_charts': 'Null',
                    'is_null_level': True
                },
                {
                    'sql_condition': 'cast(acos(case when (sin( radians("lat_col_l") ) * sin( radians("lat_col_r") ) + cos( radians("lat_col_l") ) * cos( radians("lat_col_r") ) * cos( radians("long_col_r" - "long_col_l") )) > 1 then 1 when (sin( radians("lat_col_l") ) * sin( radians("lat_col_r") ) + cos( radians("lat_col_l") ) * cos( radians("lat_col_r") ) * cos( radians("long_col_r" - "long_col_l") )) < -1 then -1 else (sin( radians("lat_col_l") ) * sin( radians("lat_col_r") ) + cos( radians("lat_col_l") ) * cos( radians("lat_col_r") ) * cos( radians("long_col_r" - "long_col_l") )) end) * 6371 as float)<= 0.1',
                    'label_for_charts': 'Distance less than 0.1km'
                },
                {
                    'sql_condition': 'cast(acos(case when (sin( radians("lat_col_l") ) * sin( radians("lat_col_r") ) + cos( radians("lat_col_l") ) * cos( radians("lat_col_r") ) * cos( radians("long_col_r" - "long_col_l") )) > 1 then 1 when (sin( radians("lat_col_l") ) * sin( radians("lat_col_r") ) + cos( radians("lat_col_l") ) * cos( radians("lat_col_r") ) * cos( radians("long_col_r" - "long_col_l") )) < -1 then -1 else (sin( radians("lat_col_l") ) * sin( radians("lat_col_r") ) + cos( radians("lat_col_l") ) * cos( radians("lat_col_r") ) * cos( radians("long_col_r" - "long_col_l") )) end) * 6371 as float)<= 1',
                    'label_for_charts': 'Distance less than 1km'
                },
                {
                    'sql_condition': 'cast(acos(case when (sin( radians("lat_col_l") ) * sin( radians("lat_col_r") ) + cos( radians("lat_col_l") ) * cos( radians("lat_col_r") ) * cos( radians("long_col_r" - "long_col_l") )) > 1 then 1 when ( sin( radians("lat_col_l") ) * sin( radians("lat_col_r") ) + cos( radians("lat_col_l") ) * cos( radians("lat_col_r") ) * cos( radians("long_col_r" - "long_col_l") )) < -1 then -1 else (sin( radians("lat_col_l") ) * sin( radians("lat_col_r") ) + cos( radians("lat_col_l") ) * cos( radians("lat_col_r") ) * cos( radians("long_col_r" - "long_col_l") )) end) * 6371 as float)<= 10',
                    'label_for_charts': 'Distance less than 10km'
                },
                {
                    'sql_condition': 'ELSE', 
                    'label_for_charts': 'All other comparisons'
                }],
            'comparison_description': 'Km distance within the following thresholds Km threshold(s): 0.1, Km threshold(s): 1, Km threshold(s): 10 vs. anything else'
        }
        ```

    Each of which gives

    ```json
    {
            'output_column_name': 'custom_lat_col_long_col',
            'comparison_levels': [
                {
                    'sql_condition': '(lat_col_l IS NULL OR lat_col_r IS NULL) \nOR (long_col_l IS NULL OR long_col_r IS NULL)',
                    'label_for_charts': 'Null',
                    'is_null_level': True
                },
                {
                    'sql_condition': 'cast(acos(case when (sin( radians("lat_col_l") ) * sin( radians("lat_col_r") ) + cos( radians("lat_col_l") ) * cos( radians("lat_col_r") ) * cos( radians("long_col_r" - "long_col_l") )) > 1 then 1 when (sin( radians("lat_col_l") ) * sin( radians("lat_col_r") ) + cos( radians("lat_col_l") ) * cos( radians("lat_col_r") ) * cos( radians("long_col_r" - "long_col_l") )) < -1 then -1 else (sin( radians("lat_col_l") ) * sin( radians("lat_col_r") ) + cos( radians("lat_col_l") ) * cos( radians("lat_col_r") ) * cos( radians("long_col_r" - "long_col_l") )) end) * 6371 as float)<= 0.1',
                    'label_for_charts': 'Distance less than 0.1km'
                },
                {
                    'sql_condition': 'cast(acos(case when (sin( radians("lat_col_l") ) * sin( radians("lat_col_r") ) + cos( radians("lat_col_l") ) * cos( radians("lat_col_r") ) * cos( radians("long_col_r" - "long_col_l") )) > 1 then 1 when (sin( radians("lat_col_l") ) * sin( radians("lat_col_r") ) + cos( radians("lat_col_l") ) * cos( radians("lat_col_r") ) * cos( radians("long_col_r" - "long_col_l") )) < -1 then -1 else (sin( radians("lat_col_l") ) * sin( radians("lat_col_r") ) + cos( radians("lat_col_l") ) * cos( radians("lat_col_r") ) * cos( radians("long_col_r" - "long_col_l") )) end) * 6371 as float)<= 1',
                    'label_for_charts': 'Distance less than 1km'
                },
                {
                    'sql_condition': 'cast(acos(case when (sin( radians("lat_col_l") ) * sin( radians("lat_col_r") ) + cos( radians("lat_col_l") ) * cos( radians("lat_col_r") ) * cos( radians("long_col_r" - "long_col_l") )) > 1 then 1 when ( sin( radians("lat_col_l") ) * sin( radians("lat_col_r") ) + cos( radians("lat_col_l") ) * cos( radians("lat_col_r") ) * cos( radians("long_col_r" - "long_col_l") )) < -1 then -1 else (sin( radians("lat_col_l") ) * sin( radians("lat_col_r") ) + cos( radians("lat_col_l") ) * cos( radians("lat_col_r") ) * cos( radians("long_col_r" - "long_col_l") )) end) * 6371 as float)<= 10',
                    'label_for_charts': 'Distance less than 10km'
                },
                {
                    'sql_condition': 'ELSE', 
                    'label_for_charts': 'All other comparisons'
                }],
            'comparison_description': 'Km distance within the following thresholds Km threshold(s): 0.1, Km threshold(s): 1, Km threshold(s): 10 vs. anything else'
    }
    ```

