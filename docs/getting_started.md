---
hide:
  - navigation
---

# Getting Started

## :material-download: Install
Splink supports python 3.7+.

To obtain the latest released version of splink you can install from PyPI using pip:
```shell
pip install splink
```

or if you prefer, you can instead install splink using conda:
```shell
conda install -c conda-forge splink
```

??? "DuckDB-less Installation"
    ### DuckDB-less Installation
    Should you be unable to install `DuckDB` to your local machine, you can still run `Splink` without the `DuckDB` dependency using a small workaround.

    To start, install the latest released version of splink from PyPI without any dependencies using:
    ```shell
    pip install splink --no-deps
    ```

    Then, to install the remaining requirements, download the following `requirements.txt` from our github repository using:
    ```shell
    github_url="https://raw.githubusercontent.com/moj-analytical-services/splink/master/scripts/duckdbless_requirements.txt"
    output_file="splink_requirements.txt"

    # Download the file from GitHub using curl
    curl -o "$output_file" "$github_url"
    ```

    Or, if you're either unable to download it directly from github or you'd rather create the file manually, simply:

    1. Create a file called `splink_requirements.txt`
    2. Copy and paste the contents from our [duckdbless requirements file](https://github.com/moj-analytical-services/splink/blob/master/scripts/duckdbless_requirements.txt) into your file.

    Finally, run the following command within your virtual environment to install the remaining splink dependencies:
    ```shell
    pip install -r splink_requirements.txt
    ```

## :rocket: Quickstart

To get a basic Splink model up and running, use the following code. It demonstrates how to:

1. Estimate the parameters of a deduplication model
2. Use the parameter estimates to identify duplicate records
3. Use clustering to generate an estimated unique person ID.

For more detailed tutorial, please see [section below](#tutorial).

???+ note "Simple Splink Model Example"
    ```py
    from splink.duckdb.linker import DuckDBLinker
    import splink.duckdb.comparison_library as cl
    import splink.duckdb.comparison_template_library as ctl
    from splink.duckdb.blocking_rule_library import block_on
    from splink.datasets import splink_datasets

    df = splink_datasets.fake_1000

    settings = {
        "link_type": "dedupe_only",
        "blocking_rules_to_generate_predictions": [
            block_on("first_name"),
            block_on("surname"),
        ],
        "comparisons": [
            ctl.name_comparison("first_name"),
            ctl.name_comparison("surname"),
            ctl.date_comparison("dob", cast_strings_to_date=True),
            cl.exact_match("city", term_frequency_adjustments=True),
            ctl.email_comparison("email", include_username_fuzzy_level=False),
        ],
    }

    linker = DuckDBLinker(df, settings)
    linker.estimate_u_using_random_sampling(max_pairs=1e6)

    blocking_rule_for_training = block_on(["first_name", "surname"])

    linker.estimate_parameters_using_expectation_maximisation(blocking_rule_for_training)

    blocking_rule_for_training = block_on("substr(dob, 1, 4)")  # block on year
    linker.estimate_parameters_using_expectation_maximisation(blocking_rule_for_training)


    pairwise_predictions = linker.predict()

    clusters = linker.cluster_pairwise_predictions_at_threshold(pairwise_predictions, 0.95)
    clusters.as_pandas_dataframe(limit=5)
    ```

## :link: Tutorials

You can learn more about Splink in the step-by-step [tutorial](./demos/00_Tutorial_Introduction.ipynb).

## :material-video: Videos

<iframe width="560" height="315" src="https://www.youtube.com/embed/msz3T741KQI" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" allowfullscreen></iframe>

## :simple-jupyter: Example Notebooks

You can see end-to-end example of several use cases in the [example notebooks](./demos/examples/examples_index.md), or by clicking the following Binder link:

[![Binder](https://mybinder.org/badge.svg)](https://mybinder.org/v2/gh/moj-analytical-services/splink_demos/master?urlpath=lab)

## :bar_chart: Charts Gallery

You can see all of the interactive charts provided in Splink by checking out the [Charts Gallery](./charts/index.md).