---
tags:
  - settings
  - Dedupe
  - Link
  - Link and Dedupe
  - Expectation Maximisation
  - Comparisons
  - Blocking Rules
---

## Guide to Splink settings

This document enumerates all the settings and configuration options available when
developing your data linkage model.


<hr>

## `link_type`

The type of data linking task.  Required.

- When `dedupe_only`, `splink` find duplicates.  User expected to provide a single input dataset.

- When `link_and_dedupe`, `splink` finds links within and between input datasets.  User is expected to provide two or more input datasets.

- When `link_only`,  `splink` finds links between datasets, but does not attempt to deduplicate the datasets (it does not try and find links within each input dataset.) User is expected to provide two or more input datasets.

**Examples**: `['dedupe_only', 'link_only', 'link_and_dedupe']`

<hr>

## `probability_two_random_records_match`

The probability that two records chosen at random (with no blocking) are a match.  For example, if there are a million input records and each has on average one match, then this value should be 1/1,000,000.

If you estimate parameters using expectation maximisation (EM), this provides an initial value (prior) from which the EM algorithm will start iterating.  EM will then estimate the true value of this parameter.

**Default value**: `0.0001`

**Examples**: `[1e-05, 0.006]`

<hr>

## `em_convergence`

Convergence tolerance for the Expectation Maximisation algorithm

The algorithm will stop converging when the maximum of the change in model parameters between iterations is below this value

**Default value**: `0.0001`

**Examples**: `[0.0001, 1e-05, 1e-06]`

<hr>

## `max_iterations`

The maximum number of Expectation Maximisation iterations to run (even if convergence has not been reached)

**Default value**: `25`

**Examples**: `[20, 150]`

<hr>

## `unique_id_column_name`

Splink requires that the input dataset has a column that uniquely identifies each record.  `unique_id_column_name` is the name of the column in the input dataset representing this unique id

For linking tasks, ids must be unique within each dataset being linked, and do not need to be globally unique across input datasets

**Default value**: `unique_id`

**Examples**: `['unique_id', 'id', 'pk']`

<hr>

## `source_dataset_column_name`

The name of the column in the input dataset representing the source dataset

Where we are linking datasets, we can't guarantee that the unique id column is globally unique across datasets, so we combine it with a source_dataset column.  Usually, this is created by Splink for the user

**Default value**: `source_dataset`

**Examples**: `['source_dataset', 'dataset_name']`

<hr>

## `retain_matching_columns`

If set to true, each column used by the `comparisons` SQL expressions will be retained in output datasets

This is helpful so that the user can inspect matches, but once the comparison vector (gamma) columns are computed, this information is not actually needed by the algorithm.  The algorithm will run faster and use less resources if this is set to false.

**Default value**: `True`

**Examples**: `[False, True]`

<hr>

## `retain_intermediate_calculation_columns`

Retain intermediate calculation columns, such as the Bayes factors associated with each column in `comparisons`

The algorithm will run faster and use less resources if this is set to false.

**Default value**: `False`

**Examples**: `[False, True]`

<hr>

## comparisons

A list specifying how records should be compared for probabilistic matching.  Each element is a dictionary

???+ note "Settings keys nested within each member of `comparisons`"

    ### output_column_name

    The name used to refer to this comparison in the output dataset.  By default, Splink will set this to the name(s) of any input columns used in the comparison.  This key is most useful to give a clearer description to comparisons that use multiple input columns.  e.g. a location column that uses postcode and town may be named location

    For a comparison column that uses a single input column, e.g. first_name, this will be set first_name. For comparison columns that use multiple columns, if left blank, this will be set to the concatenation of columns used.

    **Examples**: `['first_name', 'surname']`

    <hr>

    ### comparison_description

    An optional label to describe this comparison, to be used in charting outputs.

    **Examples**: `['First name exact match', 'Surname with middle levenshtein level']`

    <hr>

    ### comparison_levels

    Comparison levels specify how input values should be compared.  Each level corresponds to an assessment of similarity, such as exact match, Jaro-Winkler match, one side of the match being null, etc

    Each comparison level represents a branch of a SQL case expression. They are specified in order of evaluation, each with a `sql_condition` that represents the branch of a case expression

    **Example**:
    ``` json
    [{
        "sql_condition": "first_name_l IS NULL OR first_name_r IS NULL",
        "label_for_charts": "null",
        "null_level": True
    },
    {
        "sql_condition": "first_name_l = first_name_r",
        "label_for_charts": "exact_match",
        "tf_adjustment_column": "first_name"
    },
    {
        "sql_condition": "ELSE",
        "label_for_charts": "else"
    }]
    ```

    <hr>

    ??? note "Settings keys nested within each member of `comparison_levels`"

        #### `sql_condition`

        A branch of a SQL case expression without WHEN and THEN e.g. `jaro_winkler_sim(surname_l, surname_r) > 0.88`

        **Examples**: `['forename_l = forename_r', 'jaro_winkler_sim(surname_l, surname_r) > 0.88']`

        <hr>

        #### label_for_charts

        A label for this comparison level, which will appear on charts as a reminder of what the level represents

        **Examples**: `['exact', 'postcode exact']`

        <hr>

        #### u_probability

        the u probability for this comparison level - i.e. the proportion of records that match this level amongst truly non-matching records

        **Examples**: `[0.9]`

        <hr>

        #### m_probability

        the m probability for this comparison level - i.e. the proportion of records that match this level amongst truly matching records

        **Examples**: `[0.1]`

        <hr>

        #### is_null_level

        If true, m and u values will not be estimated and instead the match weight will be zero for this column.  See treatment of nulls here on page 356, quote '. Under this MAR assumption, we can simply ignore missing data.': https://imai.fas.harvard.edu/research/files/linkage.pdf

        **Default value**: `False`

        <hr>

        #### tf_adjustment_column

        Make term frequency adjustments for this comparison level using this input column

        **Default value**: `None`

        **Examples**: `['first_name', 'postcode']`

        <hr>

        #### tf_adjustment_weight

        Make term frequency adjustments using this weight. A weight of 1.0 is a full adjustment.  A weight of 0.0 is no adjustment.  A weight of 0.5 is a half adjustment

        **Default value**: `1.0`

        **Examples**: `['first_name', 'postcode']`

        <hr>

        #### tf_minimum_u_value

        Where the term frequency adjustment implies a u value below this value, use this minimum value instead

        This prevents excessive weight being assigned to very unusual terms, such as a collision on a typo

        **Default value**: `0.0`

        **Examples**: `[0.001, 1e-09]`

        <hr>


## `blocking_rules_to_generate_predictions`

A list of one or more blocking rules to apply. A Cartesian join is applied if `blocking_rules_to_generate_predictions` is empty or not supplied.

Each rule is a SQL expression representing the blocking rule, which will be used to create a join.  The left table is aliased with `l` and the right table is aliased with `r`. For example, if you want to block on a `first_name` column, the blocking rule would be

`l.first_name = r.first_name`.

To block on first name and the first letter of surname, it would be

`l.first_name = r.first_name and substr(l.surname,1,1) = substr(r.surname,1,1)`.

Note that Splink deduplicates the comparisons generated by the blocking rules.

If empty or not supplied, all comparisons between the input dataset(s) will be generated and blocking will not be used. For large input datasets, this will generally be computationally intractable because it will generate comparisons equal to the number of rows squared.

**Default value**: `[]`

**Examples**: `[['l.first_name = r.first_name AND l.surname = r.surname', 'l.dob = r.dob']]`

<hr>

## `additional_columns_to_retain`

A list of columns not being used in the probabilistic matching comparisons that you want to include in your results.

By default, Splink drops columns which are not used by any comparisons.  This gives you the option to retain columns which are not used by the model.  A common example is if the user has labelled data (training data) and wishes to retain the labels in the outputs

**Default value**: `[]`

**Examples**: `[['cluster', 'col_2'], ['other_information']]`

<hr>

## `bayes_factor_column_prefix`

The prefix to use for the columns that will be created to store the Bayes factors

**Default value**: `bf_`

**Examples**: `['bf_', '__bf__']`

<hr>

## `term_frequency_adjustment_column_prefix`

The prefix to use for the columns that will be created to store the term frequency adjustments

**Default value**: `tf_`

**Examples**: `['tf_', '__tf__']`

<hr>

## `comparison_vector_value_column_prefix`

The prefix to use for the columns that will be created to store the comparison vector values

**Default value**: `gamma_`

**Examples**: `['gamma_', '__gamma__']`

<hr>

## `sql_dialect`

The SQL dialect in which `sql_conditions` are written.  Must be a valid SQLGlot dialect

**Default value**: `None`

**Examples**: `['spark', 'duckdb', 'presto', 'sqlite']`

<hr>
