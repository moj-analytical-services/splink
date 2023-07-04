# Blocking for Model Training

Model Training Blocking Rules choose which record pairs from a dataset get considered when training a Splink model. These are used during Expectation Maximisation (EM), where we estimate the [m probability](./fellegi_sunter.md#m-probability) (in most cases).

The aim of Model Training Blocking Rules is to reduce the number of record pairs considered when training a Splink model in order to reduce the computational resource required. Each Training Blocking Rule define a training "block" of records which have a combination of matches and non-matches that are considered by Splink's Expectation Maximisation algorithm.

The Expectation Maximisation algorithm seems to work best when the pairwise record comparisons are a mix of anywhere between around 0.1% and 99.9% true matches. It works less efficiently if there is a huge imbalance between the two (e.g. a billion non matches and only a hundred matches).

Note: Unlike [Prediction Blocking Rules](./blocking_predictions.md), it does not matter if Training Blocking Rules excludes some true matches - it just needs to generate examples of matches and non-matches.


## Using Training Blocking Rules in Splink


Blocking Rules for Model Training are used as a parameter in the `estimate_parameters_using_expectation_maximisation` function. After a `linker` object has been instantiated, you can estimate `m probability` with training sessions such as:

```python

blocking_rule_for_training = "l.first_name = r.first_name"
linker.estimate_parameters_using_expectation_maximisation(
    blocking_rule_for_training
    )

```

Here, we have defined a "block" of records where `first_name` are the same. As names are not unique, we can be pretty sure that there will be a combination of matches and non-matches in this "block" which is what is required for the EM algorithm.

Matching only on `first_name` will likely generate a large "block" of pairwise comparisons which will take longer to run. In this case it may be worthwhile applying a tighter blocking rule to reduce runtime. For example, a match on `first_name` and `surname`:

```python

blocking_rule_for_training = "l.first_name = r.first_name and l.surname = r.surname"
linker.estimate_parameters_using_expectation_maximisation(
    blocking_rule_for_training
    )

```

which will still have a combination of matches and non-matches, but fewer record pairs to consider.