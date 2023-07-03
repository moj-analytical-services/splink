# Blocking for Model Training

## The purpose of the `blocking_rule` parameter on `estimate_parameters_using_expectation_maximisation`

The purpose of this blocking rule is to reduce the number of pairwise generated to a computationally-tractable number to enable the expectation maximisation algorithm to work.

The expectation maximisation algorithm seems to work best when the pairwise record comparisons are a mix of anywhere between around 0.1% and 99.9% true matches. It works less effectively if there are very few examples of either matches or non-matches. It works less efficiently if there is a huge imbalance between the two (e.g. a billion non matches and only a hundred matches).

It does not matter if this blocking rule excludes some true matches - it just needs to generate examples of matches and non matches.

Since they serve different purposes, the blocking rules most appropriate to use with `blocking_rules_to_generate_predictions` will often be different to those for `estimate_parameters_using_expectation_maximisation`, but it is also common for the same rule to be used in both places.

## Using Training Blocking Rules in Splink


What is the difference between the list of `blocking_rules_to_generate_predictions` specifed in the Splink settings dictionary, and the blocking rule that must be provided as an argument to `estimate_parameters_using_expectation_maximisation`?

These two kinds of blocking rules can be seen in the following code snippet:

=== ":simple-duckdb: DuckDB"
    ```python
    import splink.duckdb.comparison_library as cl

    settings = {
        "link_type": "dedupe_only",
        "blocking_rules_to_generate_predictions": [
            "l.first_name = r.first_name and substr(l.surname,1,1) = substr(r.surname,1,1)",
            "l.dob = r.dob",
        ],
        "comparisons": [
            cl.levenshtein_at_thresholds("first_name", 2),
            cl.exact_match("surname"),
            cl.exact_match("dob"),
            cl.exact_match("city", term_frequency_adjustments=True),
            cl.exact_match("email"),
        ],
    }


    linker = DuckDBLinker(df, settings)
    linker.estimate_u_using_random_sampling(max_pairs=1e6)

    blocking_rule_for_training = "l.first_name = r.first_name and l.surname = r.surname"
    linker.estimate_parameters_using_expectation_maximisation(blocking_rule_for_training)

    blocking_rule_for_training = "l.dob = r.dob and l.city = r.city"
    linker.estimate_parameters_using_expectation_maximisation(blocking_rule_for_training)

    ```
=== ":simple-apachespark: Spark"
    ```python
    import splink.spark.comparison_library as cl

    settings = {
        "link_type": "dedupe_only",
        "blocking_rules_to_generate_predictions": [
            "l.first_name = r.first_name and substr(l.surname,1,1) = substr(r.surname,1,1)",
            "l.dob = r.dob",
        ],
        "comparisons": [
            cl.levenshtein_at_thresholds("first_name", 2),
            cl.exact_match("surname"),
            cl.exact_match("dob"),
            cl.exact_match("city", term_frequency_adjustments=True),
            cl.exact_match("email"),
        ],
    }


    linker = SparkLinker(df, settings)
    linker.estimate_u_using_random_sampling(max_pairs=1e6)

    blocking_rule_for_training = "l.first_name = r.first_name and l.surname = r.surname"
    linker.estimate_parameters_using_expectation_maximisation(blocking_rule_for_training)

    blocking_rule_for_training = "l.dob = r.dob and l.city = r.city"
    linker.estimate_parameters_using_expectation_maximisation(blocking_rule_for_training)

    ```
=== ":simple-amazonaws: Athena"
    ```python
    import splink.athena.comparison_library as cl

    settings = {
        "link_type": "dedupe_only",
        "blocking_rules_to_generate_predictions": [
            "l.first_name = r.first_name and substr(l.surname,1,1) = substr(r.surname,1,1)",
            "l.dob = r.dob",
        ],
        "comparisons": [
            cl.levenshtein_at_thresholds("first_name", 2),
            cl.exact_match("surname"),
            cl.exact_match("dob"),
            cl.exact_match("city", term_frequency_adjustments=True),
            cl.exact_match("email"),
        ],
    }


    linker = AthenaLinker(df, settings)
    linker.estimate_u_using_random_sampling(max_pairs=1e6)

    blocking_rule_for_training = "l.first_name = r.first_name and l.surname = r.surname"
    linker.estimate_parameters_using_expectation_maximisation(blocking_rule_for_training)

    blocking_rule_for_training = "l.dob = r.dob and l.city = r.city"
    linker.estimate_parameters_using_expectation_maximisation(blocking_rule_for_training)

    ```

The answer is that they serve different purposes.