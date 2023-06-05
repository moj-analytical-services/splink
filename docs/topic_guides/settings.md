# Defining a Splink Model

## What makes a Splink Model?

When building any linkage model in Splink, there are 3 key things which need to be defined:

1. What **type of linkage** you want (defined by the [link type](link_type.md))
2. What **pairs of records** to consider (defined by [blocking rules](./blocking_rules.md))
3. What **features** to consider, and how they should be **compared** (defined by [comparisons](./customising_comparisons.ipynb))


## Defining a Splink model with a settings dictionary

All aspects of a Splink model are defined via the settings dictionary. The settings object is a json-like object which underpins a model.

For example, consider a simple model (as defined in the [README](.../README.md)). The model is defined by the following settings dictionary

```py linenums="1"
import splink.duckdb.duckdb_comparison_library as cl
import splink.duckdb.duckdb_comparison_template_library as ctl

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

The `link_type` is defined as a deduplication job for a single dataset.

```py linenums="5"
    "link_type": "dedupe_only", 
```

**2. Pairs of records to consider**

The `blocking_rules_to_generate_predictions` define a subset of pairs of records for the model to be trained on where there is a match on `first_name` or `surname`.

```py linenums="6"
    "blocking_rules_to_generate_predictions": [
        "l.first_name = r.first_name",
        "l.surname = r.surname",
    ],
```

**3. Features to consider, and how they should be compared**

The 

```py linenums="10"
    "comparisons": [
        ctl.name_comparison("first_name"),
        ctl.name_comparison("surname"),
        ctl.date_comparison("dob", cast_strings_to_date=True),
        cl.exact_match("city", term_frequency_adjustments=True),
        ctl.email_comparison("email"),
    ],
```

Using functions from the [comparison template library](comparison_templates.ipynb) and [comparison library](./customising_comparisons.ipynb#)

## Saving a trained model 

## Using a pretrained model