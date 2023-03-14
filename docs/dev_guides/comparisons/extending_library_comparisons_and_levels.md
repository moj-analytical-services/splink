---
tags:
  - API
  - comparisons
  - fuzzy-matching
---
# Extending existing comparisons and comparison levels

Creating a linkage (or deduplication) model necessitates making various comparisons between (or within) your data sets. There is some choice available in what kind of comparisons you will wish to do for the linkage problem you are dealing with. Splink comes with several [comparisons ready to use directly](../../topic_guides/customising_comparisons.html#method-1-using-the-comparisonlibrary), as well as several [comparison levels that you can use to construct your own comparison](../../topic_guides/customising_comparisons.html#method-3-comparisonlevels). You may find that within these you find yourself using a specialised version repeatedly, and would like to make a shorthand for this and contribute it to the Splink library for other users to benefit from - this page will aid you in this process.

This guide supplements [the guide for adding entirely new comparisons and comparison levels](./new_library_comparisons_and_levels.md) to show how things work when you are extending existing entries.

### Subclassing existing library comparison levels

For this example, let's consider a comparison level that returns a match on two strings within a fixed [Hamming distance](https://en.wikipedia.org/wiki/Hamming_distance).
This is a specific example of the generic [string distance function comparison level](../../comparison_level_library.html#splink.comparison_level_library.DistanceFunctionLevelBase).

In this case, working in [`splink/comparison_level_library.py`](https://github.com/moj-analytical-services/splink/blob/master/splink/comparison_level_library.py), we simply subclass the appropriate level, and call its constructor, fixing whatever properties we need
(using dialect-specific properties where appropriate - in this case the name of the function which calculates Hamming distance, which will be stored in the property `self._hamming_name`):

```python
class HammingLevelBase(DistanceFunctionLevelBase):
    def __init__(
        self,
        col_name: str,
        distance_threshold: int,
        m_probability=None,
    ):
        """Represents a comparison using a Hamming distance function,

        Args:
            col_name (str): Input column name
            distance_threshold (Union[int, float]): The threshold to use to assess
                similarity
            m_probability (float, optional): Starting value for m probability.
                Defaults to None.

        Returns:
            ComparisonLevel: A comparison level that evaluates the
                Hamming similarity
        """
        super().__init__(
            col_name,
            self._hamming_name,
            distance_threshold,
            higher_is_more_similar=False,
            m_probability=m_probability,
        )
```

The rest of the process is identical to [that described for creating brand-new comparison levels](./new_library_comparisons_and_levels.html#creating-new-comparison-levels).

### Subclassing existing library comparisons

As in our `hamming_level` example above, you can similarly subclass existing library `Comparisons` to create e.g. `hamming_at_thresholds` from the more generic `distance_function_at_thresholds`, similarly to how we [create new comparisons](./new_library_comparisons_and_levels.html#creating-new-comparisons). The main difficulty here is that the subclassed comparison levels will have different function arguments, so we need to check within the constructor if we are in the generic version (`distance_function_at_thresholds`) or a specific version (`hamming_level`). See the [source code for `DistanceFunctionAtThreholdsComparisonBase`](https://github.com/moj-analytical-services/splink/blob/master/splink/comparison_library.py) for an example.
