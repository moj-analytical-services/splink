# Blocking Rules for Splink Predictions

The purpose of these blocking rules is to try and ensure that pairwise record comparisons are generated for all true matches.

## Using Prediction Blocking Rules in Splink

Blocking Rules for Prediction are defined through the `blocking_rules_to_generate_predictions` parameter in the Settings dictionary of a model. For example:

``` py hl_lines="3-6"
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
```

will generate comparisons for all true matches where names match. But it would miss a true match where there was a typo in (say) the first name.

In general, it is usually impossible to find a single rule which both:

- Reduces the number of comparisons generated to a computatally tractable number

- Ensures comparisons are generated for all true matches

This is why `blocking_rules_to_generate_predictions` is a list. Suppose we also block on `postcode`:

```python
settings = {
    "blocking_rules_to_generate_predictions" [
        "l.first_name = r.first_name and l.surname = r.surname",
        "l.postcode = r.postcode"
        ]
}
```

We will now generate a pairwise comparison for the record where there was a typo in the first name, so long as there isn't also a difference in the postcode.

By specifying a variety of `blocking_rules_to_generate_predictions`, it becomes implausible that a truly matching record would not be captured by at least one of the rules.

Note that Splink automatically deduplicates the record comparisons it generates. So, in the example above, the `"l.postcode = r.postcode"` blocking rule generates only records comparisons that were not already captured by the `first_name` and `surname` rule.