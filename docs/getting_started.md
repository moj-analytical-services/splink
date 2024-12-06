---
hide:
  - navigation
---

# Getting Started

## :material-download: Install
Splink supports python 3.8+.

To obtain the latest released version of Splink you can install from PyPI using pip:
```shell
pip install splink
```

or if you prefer, you can instead install Splink using conda:
```shell
conda install -c conda-forge splink
```

??? "Backend Specific Installs"
    ### Backend Specific Installs
    From Splink v3.9.7, packages required by specific Splink backends can be optionally installed by adding the `[<backend>]` suffix to the end of your pip install.

    **Note** that SQLite and DuckDB come packaged with Splink and do not need to be optionally installed.

    The following backends are supported:

    === ":simple-apachespark: Spark"
        ```sh
        pip install 'splink[spark]'
        ```

    === ":simple-amazonaws: Athena"
        ```sh
        pip install 'splink[athena]'
        ```

    === ":simple-postgresql: PostgreSQL"
        ```sh
        pip install 'splink[postgres]'
        ```



## :rocket: Quickstart

To get a basic Splink model up and running, use the following code. It demonstrates how to:

1. Estimate the parameters of a deduplication model
2. Use the parameter estimates to identify duplicate records
3. Use clustering to generate an estimated unique person ID.

???+ note "Simple Splink Model Example"
    ```py
    import splink.comparison_library as cl
    from splink import DuckDBAPI, Linker, SettingsCreator, block_on, splink_datasets

    db_api = DuckDBAPI()

    df = splink_datasets.fake_1000

    settings = SettingsCreator(
        link_type="dedupe_only",
        comparisons=[
            cl.NameComparison("first_name"),
            cl.JaroAtThresholds("surname"),
            cl.DateOfBirthComparison(
                "dob",
                input_is_string=True,
            ),
            cl.ExactMatch("city").configure(term_frequency_adjustments=True),
            cl.EmailComparison("email"),
        ],
        blocking_rules_to_generate_predictions=[
            block_on("first_name", "dob"),
            block_on("surname"),
        ]
    )

    linker = Linker(df, settings, db_api)

    linker.training.estimate_probability_two_random_records_match(
        [block_on("first_name", "surname")],
        recall=0.7,
    )

    linker.training.estimate_u_using_random_sampling(max_pairs=1e6)

    linker.training.estimate_parameters_using_expectation_maximisation(
        block_on("first_name", "surname")
    )

    linker.training.estimate_parameters_using_expectation_maximisation(block_on("email"))

    pairwise_predictions = linker.inference.predict(threshold_match_weight=-5)

    clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(
        pairwise_predictions, 0.95
    )

    df_clusters = clusters.as_pandas_dataframe(limit=5)
    ```

If you're using an LLM to suggest Splink code, see [here](./topic_guides/llms/prompting_llms.md) for suggested prompts and context.

## Tutorials

You can learn more about Splink in the step-by-step [tutorial](./demos/tutorials/00_Tutorial_Introduction.ipynb). Each has a corresponding Google Colab link to run the notebook in your browser.

## Example Notebooks

You can see end-to-end example of several use cases in the [example notebooks](./demos/examples/examples_index.md). Each has a corresponding Google Colab link to run the notebook in your browser.

## Getting help

If after reading the documentatation you still have questions, please feel free to post on our [discussion forum](https://github.com/moj-analytical-services/splink/discussions).