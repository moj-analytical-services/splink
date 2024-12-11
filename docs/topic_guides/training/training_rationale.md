---
tags:
  - Training
---

In Splink, in most scenarios, we recommend a hybrid approach to training model parameters, whereby we use direct estimation techniques for the `probability_two_random_records_match` (λ) parameter and the `u` probabilities, and then use EM training for the `m` probabilities.

The overall rationale is that we found that whilst it's possible to train all parameters using EM, empirically we've found you get better parameter estimates, and fewer convergence problems using direct estimation of some parameters.

In particular:

- You can precisely estimate the `u` probabilities in most cases, so there's no reason to use a less reliable unsupervised technique.
- With `probability_two_random_records_match`, we [found that](https://github.com/moj-analytical-services/splink/issues/462) Expectation Maximisation often resulted in inaccurate results due to our 'blocking' methodology for training `m` values. In practice, the direct estimation technique gave better results, despite being somewhat imprecise.

The recommended sequence for model training and associated rationale is as follows:

### 1. Use [linker.training.estimate_probability_two_random_records_match](https://moj-analytical-services.github.io/splink/api_docs/training.html#splink.internals.linker_components.training.LinkerTraining.estimate_probability_two_random_records_match) to estimate the proportion of records.

The `probability_two_random_records_match` is one of the harder parameters to estimate because there's a catch-22: to know its value, we need to know which records match, but that's the whole problem we're trying to solve.

Luckily, in most cases it's relatively easy to come up with a good guess, within (say) half or double its true value. It turns out that this is good enough to get good estimates of the other parameters, and ultimatey to get good predictions.

In our methodology,  the user specifies a list of deterministic matching rules that they believe represent true matches. These will be strict, and therefore will miss some fuzzy matches. For all but the most messy datasets, they should capture the majority of matches. A recall parameter is then provided by the user, which is the user's guess of how many matches are missed by these rules.

In a typical case, the deterministic rules may capture (say) 80% of matches. If the user gets this wrong and provides recall of say 60% or 95%, the effect on `probability_two_random_records_match` is not huge.

For example, a typical parameter estimate for `probability_two_random_records_match` when expressed as a match weight may be -14.  Assuming true recall of 80%, if the user guessed recall wrong at 60% or 95%, the parameter would be estimated at -13.75 or -14.42 respectively.

In turn, so long as this parameter is roughly right, it serves as an 'anchor' to EM parameter estimation later, which prevents it iterating/converging to the 'wrong' place (there's no guarantee it converges to a global minimum, only a local one).

Example:
```
deterministic_rules = [
    block_on("first_name", "surname", "dob"),
    block_on("email")"
]

linker.training.estimate_probability_two_random_records_match(deterministic_rules, recall=0.7)
```

### 2. Use [linker.training.estimate_u_using_random_sampling](https://moj-analytical-services.github.io/splink/api_docs/training.html#splink.internals.linker_components.training.LinkerTraining.estimate_u_using_random_sampling) to train `u` probabilities.

This one is easy to justify: On a sufficiently large dataset, if you take two random records, they will almost certainly not be a match. The `u` probabilities are calculated on the basis of truly non-matching records. So we can directly calculate them from random record comparisons. The only errors will be:

- Sampling error—in which case we can increase our sample size.
- The small errors introduced by the fact that, occasionally, our sampled records will match. In practice, this rarely has a big effect.

Example:
```
linker.training.estimate_u_using_random_sampling(max_pairs=1e7)
```
Increase `max_pairs` if you need more precision. This step is usually straightforward and reliable.

### 3. Use [linker.training.estimate_parameters_using_expectation_maximisation](https://moj-analytical-services.github.io/splink/api_docs/training.html#splink.internals.linker_components.training.LinkerTraining.estimate_parameters_using_expectation_maximisation) to estimate `m` probabilities.

The `m` probabilities have the same 'catch-22' problem as the `probability_two_random_records_match`. Luckily, the magic of EM is that it [solves this problem](https://www.robinlinacre.com/em_intuition/).

In the context of record linkage on large datsets, one problem is that we cannot easily run EM on random pairs of records, because they are almost all non-matches. It would work if we had infinite computing resources and could create unlimited numbers of comparisons. But in a typical case, even if we create 1 billion random comparisons, perhaps only 1000 are matches. Now we only have a sample size of 1,000 to estimate our `m` values, and their estimates may be imprecise. This contrasts to `u` estimation, where we have a sample size of `max_pair`s , often millions or even billions.

To speed this up, we can use a trick. By blocking on some columns (e.g. first name and surname), we now restrict our record comparisons to a subset with a far higher proportion of matches than random comparisons.

This trick is vulnerable to the criticism that we may get a biased selection of matching records. The Fellegi-Sunter model assumes [columns are independent conditional on match status](https://www.robinlinacre.com/maths_of_fellegi_sunter/), which is rarely true in practice.

But if this assumption holds then the selection of records is unbiased, and the parameter estimates are correct.

#### The 'round robin'

One complexity of this approach is that when we block on first name and surname, we can't get parameter estimates for these two columns because we've forced all comparison to be equal.

That's why we need multiple EM training passes: we need a second pass blocking on e.g. date_of_birth to get parameter estimates for first_name and surname.

```
linker.training.estimate_parameters_using_expectation_maximisation(block_on("dob"))
linker.training.estimate_parameters_using_expectation_maximisation(block_on("first_name", "surname"))
```

At this point, we have:

- One estimate for the `m` values for first_name and surname
- One estimate for the `m` values for date_of_birth
- Two estimates for each `m` value for any other columns

If we're worried about our estimates, we can run more training rounds to get more estimates, e.g. blocking on postcode. We can then use [linker.visualisations.parameter_estimate_comparisons_chart](https://moj-analytical-services.github.io/splink/api_docs/visualisations.html#splink.internals.linker_components.visualisations.LinkerVisualisations.parameter_estimate_comparisons_chart) to check that the various estimates are similar. Under the hood, Splink will take an average.

```
linker.visualisations.parameter_estimate_comparisons_chart()
```

Empirically, one of the nice things here is that, because we've fixed the `u` probabilities and the `probability_two_random_records_match` at sensible values, they anchor the EM training process and it turns out you don't get too many convergence problems.

Finally, if the user is still having convergence problems, there are two options:
1. In practice, we have found these can be often due to data quality issues e.g. `mr` and `mrs` in `first_name`, so it's a good idea to go back to exploratory data analysis and understand if there's a root cause
2. If there's no obvious cause, the user can fix the m values, see [here](https://github.com/moj-analytical-services/splink/pull/2379)  and [here](https://github.com/moj-analytical-services/splink/discussions/2512#discussioncomment-11303080)