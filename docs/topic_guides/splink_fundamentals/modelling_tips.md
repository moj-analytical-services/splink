---
tags:
  - Modelling
---

# Next steps:Top Tips for Building your own model

This page summarises some recommendations for how to approach building a new Splink model, to get an accurate model as quickly as possible.

At a high level, we recommend beginning with a small sample and a basic model, then iteratively adding complexity to resolve issues and improve performance.

## General workflow

- **For large datasets, start by linking a small non-random sample**. Building a model is often a highly iterative process and you don't want long processing times slowing down your iteration cycle. Most of the modelling can be conducted on a small sample, and only once that's working, re-run everything on the full dataset.  You need a **non-random** sample of about 10,000 records. By non-random, I mean a sample that retains lots of matches - for instance, all people aged over 70, or all people with a first name starting with the characters 'pa'.  You should aim to be able to run your full training and prediction script in less than a minute. Remember to set a lower value (say `1e6`) of the `target_rows` when calling `estimate_u_using_random_sampling()` during this iteration process, but then increase in the final full-dataset run to a much higher value, maybe `1e8`.

- **Start simple, and iterate**.  It's often tempting to start by a complex model, with many granular comparison levels, in an attempt to reflect the real world closely.  Instead, I recommend starting with a simple, rough and ready model where most comparisons have 2-3 levels (exact match, possibly a fuzzy level, and everything else).  The purpose is to get to the point of looking at prediction results as quickly as possible using e.g. the comparison viewer.  You can then start to look for where your simple model is getting it wrong, and use that as the basis for improving your model, and iterating until you're seeing good results.

## Blocking

- **Many strict `blocking_rules_for_prediction` are generally better than few loose rules.**  Each individual blocking rule is likely to exclude many true matches.  But between them, it should be implausible that a truly matching record 'falls through' all the blockinges.  Many of our models have between about 10-15 `blocking_rules_for_prediction`

- **Analyse the number of comparisons before running predict**.  Use the tools in `splink.blocking_analysis` to validate that your rules aren't going to create a vast number of comparisons before asking Splink to create those comparisons.

## EM trainining

- *Predictions usually aren't very sensitive to `m` probabilities being a bit wrong*.  The hardest model parameters to estimate are the `m` probabilities.  It's fairly common for Expectation Maximisation to yield 'bad' (implausble) values.  Luckily, the accuracy of your model is usually not particularly sensitive to the `m` probabilities - the `u` probabilities drive the match weights.  In many cases, you'll get good results to simply set the `m` probabilities by by 'expert judgement' (i.e. guess).

- *Convergece problems are often indicative of the need for further data cleaning*.  Whilst predictions often aren't terribly sensitive to `m` probabilities, question why the estimation procedue is producing bad parameter estimates.  To do this, it's often enough to look at a variety of predictions to see if you can spot edge cases where the model is not doing what's expected.  For instance, we may find matches where the first name is `Mr.`.  By fixing this and reestimating, the parameter estimates make more sense.

- **Blocking rules for EM training do not need high recall**.  The purpose of blocking rules for EM training is to find a subset of records which include a reasonably balanced mix of matches and non matches.  There is no requirement that these records neet to contain all even most of the matches.  To double check that parameter estimates are a result of a biased sample of matches, you can use `linker.visualisations.parameter_estimate_comparisons_chart`.

## Working with large datasets

To optimise memory usage and performance:

- **Avoid pandas for input/output** Whilst Splink supports inputs as pandas dataframes, and you can convert results to pandas using `.as_pandas_dataframe()`, we recommend against this for large datasets.  For large datasets, use the concept of a dataframe that's native to your database backend.  For example, if you're using Spark, it's best to read your files using Spark and pass Spark dataframes into Splink, and save any outputs using `splink_dataframe.as_spark_dataframe`.  With duckdb use the inbuilt duckdb csv/parquet reader, and output via `splinkdataframe.as_duckdbpyrelation`.

- **Avoid pandas for data cleaning**.  You will generally get substantially better performance by performing data cleaning in SQL using your chosen backend rather than using pandas.

