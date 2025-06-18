# Comparison and ComparisonLevels



## Comparing information

To find matching records, Splink creates pairwise record comparisons from the input records, and scores these comparisons.

Suppose for instance your data contains `first_name` and `surname` and `dob`:

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

## Defining similarity


How how should we assess similarity between the records?

In Splink, we will use different measures of similarity for different columns in the data, and then combine these measures to get an overall similarity score.  But the most appropriate definition of similarity will differ between columns.

For example, two surnames that differ by a single character would usually be considered to be similar.  But a one character difference in a 'gender' field encoded as `M` or `F` is not similar at all!

To allow for this, Splink uses the concepts of `Comparison`s and `ComparisonLevel`s.  Each `Comparison` usually measures the similarity of a single column in the data, and each `Comparison` is made up of one or more `ComparisonLevel`s.

Within each `Comparison` are _n_ discrete `ComparisonLevel`s.  Each `ComparisonLevel` defines a discrete gradation (category) of similarity within a Comparison.  There can be as many `ComparisonLevels` as you want. For example:

```
Data Linking Model
├─-- Comparison: Gender
│    ├─-- ComparisonLevel: Exact match
│    ├─-- ComparisonLevel: All other
├─-- Comparison: First name
│    ├─-- ComparisonLevel: Exact match on surname
│    ├─-- ComparisonLevel: surnames have JaroWinklerSimilarity > 0.95
│    ├─-- ComparisonLevel: All other
```

The categories are discrete rather than continuous for performance reasons - so for instance, a `ComparisonLevel` may be defined as `jaro winkler similarity between > 0.95`, as opposed to using the Jaro-Winkler score as a continuous measure directly.

It is up to the user to decide how best to define similarity for the different columns (fields) in their data, and this is a key part of modelling a record linkage problem.

A much more detailed of how this works can be found in [this series of interactive tutorials](https://www.robinlinacre.com/probabilistic_linkage/) - refer in particular to [computing the Fellegi Sunter model](https://www.robinlinacre.com/computing_fellegi_sunter/).

## An example:


The concepts of `Comparison`s and `ComparisonLevel`s are best explained using an example.

Consider the following simple data linkage model with only two columns (in a real example there would usually be more):

```
Data Linking Model
├─-- Comparison: Date of birth
│    ├─-- ComparisonLevel: Exact match
│    ├─-- ComparisonLevel: One character difference
│    ├─-- ComparisonLevel: All other
├─-- Comparison: First name
│    ├─-- ComparisonLevel: Exact match on first_name
│    ├─-- ComparisonLevel: first_names have JaroWinklerSimilarity > 0.95
│    ├─-- ComparisonLevel: first_names have JaroWinklerSimilarity > 0.8
│    ├─-- ComparisonLevel: All other
```


In this model we have two `Comparison`s: one for date of birth and one for first name:

For date of birth, we have chosen three discrete `ComparisonLevel`s to measure similarity.  Either the dates of birth are an exact match, they differ by one character, or they are different in some other way.

For first name, we have chosen four discrete `ComparisonLevel`s to measure similarity.  Either the first names are an exact match, they have a JaroWinkler similarity of greater than 0.95, they have a JaroWinkler similarity of greater than 0.8, or they are different in some other way.

Note that these definitions are mutually exclusive, because they're implemented by Splink like an if statement.  For example, for first name, the `Comparison` is equivalent to the following pseudocode:

```python
if first_name_l_ == first_name_r:
    return "Assign to category: Exact match"
elif JaroWinklerSimilarity(first_name_l_, first_name_r) > 0.95:
    return "Assign to category: JaroWinklerSimilarity > 0.95"
elif JaroWinklerSimilarity(first_name_l_, first_name_r) > 0.8:
    return "Assign to category: JaroWinklerSimilarity > 0.8"
else:
    return "Assign to category: All other"
```

In the [next section](./customising_comparisons.ipynb), we will see how to define these `Comparison`s and `ComparisonLevel`s in Splink.
