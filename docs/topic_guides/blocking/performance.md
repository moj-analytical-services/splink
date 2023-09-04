# Blocking Rule Performance

When considering computational performance of blocking rules, there are two main drivers to address:

- How may pairwise comparisons are generated
- How quickly each pairwise comparison takes to run

Below we run through an example of how to address each of these drivers.

## Strict vs lenient Blocking Rules

One way to reduce the number of comparisons being considered within a model is to apply strict blocking rules. However, this can have a significant impact on the how well the Splink model works.

In reality, we recommend getting a model up and running with strict Blocking Rules and incrementally loosening them to see the impact on the runtime and quality of the results. By starting with strict blocking rules, the linking process will run faster which will means you can iterate through model versions more quickly.

??? example "Example - Incrementally loosening Prediction Blocking Rules"

    When choosing Prediction Blocking Rules, consider how `blocking_rules_to_generate_predictions` may be made incrementally less strict. We may start with the following rule:

    `l.first_name = r.first_name and l.surname = r.surname and l.dob = r.dob`.

    This is a very strict rule, and will only create comparisons where full name and date of birth match. This has the advantage of creating few record comparisons, but the disadvantage that the rule will miss true matches where there are typos or nulls in any of these three fields.

    This blocking rule could be loosened to:

    `substr(l.first_name,1,1) = substr(r.first_name,1,1) and l.surname = r.surname and l.year_of_birth = r.year_of_birth`

    Now it allows for typos or aliases in the first name, so long as the first letter is the same, and errors in month or day of birth.

    Depending on the side of your input data, the rule could be further loosened to

    `substr(l.first_name,1,1) = substr(r.first_name,1,1) and l.surname = r.surname`

    or even

    `l.surname = r.surname`

    The user could use the `linker.count_num_comparisons_from_blocking_rule()` function to select which rule is appropriate for their data.

## Efficient Blocking Rules

While the number of pariwise comparisons is important for reducing the computation, it is also helpful to consider the efficiency of the Blocking Rules. There are a number of ways to define subsets of records (i.e. "blocks"), but they are not all computationally efficient.

From a performance prespective, here we consider two classes of blocking rule:

- Equi-join conditions
- Filter conditions

### Equi-join Conditions

Equi-joins are simply equality conditions between records, e.g.

`l.first_name = r.first_name`

These equality-based blocking rules are extremely efficient and can be executed quickly, even on very large datasets.

Equality-based blocking rules should be considered the default method for defining blocking rules and form the basis of the [Blocking Rules Library](../../blocking_rule_library.md). For example, the above example can be written as:

`brl.block_on("first_name")`


### Filter Conditions

Filter conditions refer to any Blocking Rule that isn't a simple equality between columns. E.g.

`levenshtein(l.surname, r.surname) < 3`

Similarity based blocking rules, such as the example above, are inefficient as the `levenshtein` function needs to be evaluated for all possible record comparisons before filtering out the pairs that do not satisfy the filter condition.


### Combining Blocking Rules Efficiently

Just as how Blocking Rules can impact on performance, so can how they are combined. The most efficient Blocking Rules combinations are "AND" statements. E.g.

`l.first_name = r.first_name AND l.surname = r.surname`

"OR" statements are not as efficient and should be used sparingly. E.g.

`l.first_name = r.first_name OR l.surname = r.surname`

In most SQL engines, an `OR` condition within a blocking rule will result in all possible record comparisons being generated.  That is, the whole blocking rule becomes a filter condition rather than an equi-join condition, so these should be avoided.  For further information, see [here](https://github.com/moj-analytical-services/splink/discussions/1417#discussioncomment-6420575).

??? note "Spark-specific Further Reading"

    Given the ability to parallelise operations in Spark, there are some additional configuration options which can improve performance of blocking. Please refer to the Spark Performance Topic Guides for more information.

    Note: In Spark Equi-joins are implemented using hash partitioning, which facilitates splitting the workload across multiple machines.