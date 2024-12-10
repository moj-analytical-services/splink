
# Next steps: Tips for Building your own model

Now that you've completed the tutorial, this page summarises some recommendations for how to approach building your own Splink models.

These recommendations should help you create an accurate model as quickly as possible.  They're particularly applicable if you're working with large datasets, where you can get slowed down by long processing times.

In a nutshell, we recommend beginning with a small sample and a basic model, then iteratively adding complexity to resolve issues and improve performance.

## General workflow

- **For large datasets, start by linking a small non-random sample of records**. Building a model is an iterative process of writing data cleaning code, training models, finding issues, and circling back to fix them. You don't want long processing times slowing down this iteration cycle.

    Most of your code can be developed against a small sample of records, and only once that's working, re-run everything on the full dataset.

    You need a **non-random** sample of perhaps about 10,000 records. The same must be  non-random because it must retain lots of matches - for instance, retain all people aged over 70, or all people with a first name starting with the characters `pa`.  You should aim to be able to run your full training and prediction script in less than a minute.

    Remember to set a lower value (say `1e6`) of the `target_rows` when calling `estimate_u_using_random_sampling()` during this iteration process, but then increase in the final full-dataset run to a much higher value, maybe `1e8`, since large value of `target_rows` can cause long processing times even on relatively small datasets.

- **Start with a simple model**.  It's often tempting to start by designing a complex model, with many granular comparison levels in an attempt to reflect the real world closely.

    Instead, we recommend starting with with a simple, rough and ready model where most comparisons have 2-3 levels (exact match, possibly a fuzzy level, and everything else).  The idea is to get to the point of looking at prediction results as quickly as possible using e.g. the comparison viewer.  You can then start to look for where your simple model is getting it wrong, and use that as the basis for improving your model, and iterating until you're seeing good results.

## Blocking rules for prediction

- **Analyse the number of comparisons before running predict**.  Use the tools in `splink.blocking_analysis` to validate that your rules aren't going to create a vast number of comparisons before asking Splink to create those comparisons.

- **Many strict `blocking_rules_for_prediction` are generally better than few loose rules.**  Whilst individually, strict blocking rules are likely to exclude many true matches, between them it should be implausible that a truly matching record 'falls through' all the rules.  Many strict rules often result in far fewer overall comparisons and a small number of loose rules.  In practice, many of our real-life models have between about 10-15 `blocking_rules_for_prediction`.


## EM trainining

- **Predictions usually aren't very sensitive to `m` probabilities being a bit wrong**.  The hardest model parameters to estimate are the `m` probabilities.  It's fairly common for Expectation Maximisation to yield 'bad' (implausble) values.  Luckily, the accuracy of your model is usually not particularly sensitive to the `m` probabilities - it usually the `u` probabilities driving the biggest match weights.  If you're having problems, consider fixing some `m` probabilities by expert judgement - see [here](https://github.com/moj-analytical-services/splink/pull/2379) for how.

- **Convergence problems are often indicative of the need for further data cleaning**.  Whilst predictions often aren't terribly sensitive to `m` probabilities, question why the estimation procedue is producing bad parameter estimates.  To do this, it's often enough to look at a variety of predictions to see if you can spot edge cases where the model is not doing what's expected.  For instance, we may find matches where the first name is `Mr`.  By fixing this and reestimating, the parameter estimates often make more sense.

- **Blocking rules for EM training do not need high recall**.  The purpose of blocking rules for EM training is to find a subset of records which include a reasonably balanced mix of matches and non matches.  There is no requirement that these records contain all, or even most of the matches.  For more see [here](https://moj-analytical-services.github.io/splink/topic_guides/blocking/model_training.html)  To double check that parameter estimates are a result of a biased sample of matches, you can use `linker.visualisations.parameter_estimate_comparisons_chart`.

## Working with large datasets

To optimise memory usage and performance:

- **Avoid pandas for input/output** Whilst Splink supports inputs as pandas dataframes, and you can convert results to pandas using `.as_pandas_dataframe()`, we recommend against this for large datasets.  For large datasets, use the concept of a dataframe that's native to your database backend.  For example, if you're using Spark, it's best to read your files using Spark and pass Spark dataframes into Splink, and save any outputs using `splink_dataframe.as_spark_dataframe`.  With duckdb use the inbuilt duckdb csv/parquet reader, and output via `splinkdataframe.as_duckdbpyrelation`.

- **Avoid pandas for data cleaning**.  You will generally get substantially better performance by performing data cleaning in SQL using your chosen backend rather than using pandas.

- **Turn off intermediate columns when calling `predict()`**.  Whilst during the model development phase, it is useful to set `retain_intermediate_calculation_columns=True` and
    `retain_intermediate_calculation_columns_for_prediction=True` in your settings, you should generally turn these off when calling `predict()`.  This will result in a much smaller table as your result set.  If you want waterfall charts for individual pairs, you can use [`linker.inference.compare_two_records`](../../api_docs/inference.md)

