---
tags:
  - API
  - comparisons
---
# Creating new comparisons and comparison levels for libraries

The Fellegi-Sunter model that Splink implements depends on having several comparisons, which are each composed of two or more comparison levels.
Splink provides several _ready-made_ [comparisons](../../comparison_library.html) and [comparison levels](../../comparison_level_library.html) to use out-of-the-box, but you may find in your particular application that you have to [create your own custom versions](../../topic_guides/customising_comparisons.html#method-4-providing-the-spec-as-a-dictionary) if there is not a suitable comparison/level for the [SQL dialect you are working with](../../topic_guides/backends.html) (or for any available dialects).

Having created a custom comparison you may decide that your use case is common enough that you want to contribute it to Splink for other users to benefit from. This guide will take you through the process of doing so. Looking at existing examples should also prove to be useful for further guidance, and to perhaps serve as a starting template.

After creating your new levels/comparisons, be sure to add some tests to help ensure that the code runs without error and behaves as expected.

This guide is for adding new comparisons and comparison levels from scratch. If you instead want to create specialised versions of existing levels, be sure to also have a look at the [guide for extending existing library entries](./extending_library_comparisons_and_levels.html).

## Creating new comparison levels

For this example, let's consider a comparison level that compares if the length of two arrays are within `n` of one another (without reference to the contents of these arrays) for some non-negative integer `n`. An example of this might be if we were linking people with partial address information in two tables --- in one table we have an array of postcodes, and the other table we have an array of road-names. We don't expect them to match, but we are probably interested if the count of the number of objects within each array are similar - each corresponding to the number of addresses per person.

To create a new comparison level, you must create a new subclass of `ComparisonLevel` which will serve as the base comparison level for any SQL dialects that will allow this level, in the file [`splink/comparison_level_library.py`](https://github.com/moj-analytical-services/splink/blob/master/splink/comparison_level_library.py).
It will contain the full logic for creating the comparison level - any dialect dependencies will be implemented as properties on the specific dialect-dependent object.
In this case we will need to refer to a property `_array_length_function_name`, as this can vary by dialect.
We will not define the property directly on this object, as our dialect-dependent versions will inherit this property from elsewhere.
We also include any customisable parameters for our level - in this case we will allow options for the maximum number of elements the array may differ by.

```python
class ArrayLengthLevelBase(ComparisonLevel):
    def __init__(
        self,
        col_name: str,
        length_difference: int,
        m_probability=None,
    ):
        """Compares two arrays whose sizes are within a fixed distance
        | length(arr_1) - length(arr_2) | <= (length_difference)

        Arguments:
            col_name (str): Input column name
            length_difference (int): Maximum difference in array lengths
            m_probability (float, optional): Starting value for m probability.
                Defaults to None.

        Returns:
            ComparisonLevel: A comparison level that evaluates the size difference
                between two arrays
        """
        col = InputColumn(col_name, sql_dialect=self._sql_dialect)
        col_l, col_r = col.names_l_r()

        sql_exp = (
            f"abs("
            f"{self._array_length_function_name}({col_l}) - "
            f"{self._array_length_function_name}({col_r})"
            f") <= {length_difference}"
        )
        level_dict = {
            "sql_condition": sql_exp,
            "label_for_charts": f"Array sizes differ by at most {length_difference)",
        }

        if m_probability:
            level_dict["m_probability"] = m_probability

        super().__init__(level_dict, sql_dialect=self._sql_dialect)
```

If you are using a new dialect-dependent property (as we are in this case), then it should be added as a property on `DialectBase` in [`splink.dialect_base.py`](https://github.com/moj-analytical-services/splink/blob/master/splink/dialect_base.py),
with either a sensible default value, or raising a `NotImplementedError`:

```python
class DialectBase():
    ...
    @property
    def _array_length_function_name():
        raise NotImplementedError(
            "`array_length_function_name` not implemented on base class"
        )
```

Then any dialects that use a different value can override this (e.g in [`splink.spark.spark_helpers.spark_base`](https://github.com/moj-analytical-services/splink/blob/master/splink/spark/spark_helpers/spark_base.py)):

```python
class SparkBase(DialectBase):
    ...
    @property
    def _array_length_function_name():
        return "array_size"
```

Then any dialects where this comparison level can be used can simply inherit from this dialect-specific base, along with the comparison level base `ArrayLengthLevelBase` - here in [`splink.spark.spark_helpers.spark_comparison_imports`](https://github.com/moj-analytical-services/splink/blob/master/splink/spark/spark_helpers/spark_comparison_imports.py):

```python
class array_length_level(SparkBase, ArrayLengthLevelBase):
    pass
```

Similarly for DuckDB define the appropriate function name in the base [`splink.duckdb.duckdb_helpers.duckdb_base`](https://github.com/moj-analytical-services/splink/blob/master/splink/duckdb/duckdb_helpers/duckdb_base.py)

```python
class DuckDBBase(DialectBase):
    ...
    @property
    def _array_length_function_name():
        return "array_length"
```

and then simply create the level in the corresponding library [`splink.duckdb.duckdb_helpers.duckdb_comparison_imports`](https://github.com/moj-analytical-services/splink/blob/master/splink/duckdb/duckdb_helpers/duckdb_comparison_imports.py):

```python
class array_length_level(DuckDBBase, ArrayLengthLevelBase):
    pass
```

The names of these should be the same for all dialects (and written in snake-case), with them being distinguished solely by path.

## Creating new comparisons

The process for creating new library `Comparison`s is similar to the `ComparisonLevel` case, but slightly more involved.
This is due to the fact that dialect-specific `Comparison`s need to 'know' about
the dialect-specific `ComparisonLevel`s that they employ.

As an example, we will consider a new `Comparison` that makes use of our new `array_length_level` above.
Specifically, it will have the following levels:

* an optional `exact_match_level`
* one or more `array_length_level`s with different values of `length_difference`, as specified
* an `else_level`

```python
class ArrayLengthAtThresholdsBase(Comparison):
    def __init__(
        self,
        col_name: str,
        length_thresholds: Union[int, list] = [0],
        include_exact_match_level=True,
        term_frequency_adjustments=False,
        m_probability_exact_match=None,
        m_probability_or_probabilities_sizes: Union[float, list] = None,
        m_probability_else=None,
    ):
        """A comparison of the data in the array column `col_name` with various
        size difference thresholds to assess similarity levels.

        An example of the output with default arguments and settings
        `length_thresholds = [0]` would be
        - An exact match
        - The two arrays are the same length
        - Anything else (i.e. the arrays are difference lengths)

        Args:
            col_name (str): The name of the array column to compare.
            length_thresholds (Union[int, list], optional): The difference(s) between
                array sizes of thresholds, to assess whether two arrays are within a
                given length difference.
            include_exact_match_level (bool, optional): If True, include an exact match
                level. Defaults to True.
            term_frequency_adjustments (bool, optional): If True, apply term frequency
                adjustments to the exact match level. Defaults to False.
            m_probability_exact_match (float, optional): If provided, overrides the
                default m probability for the exact match level. Defaults to None.
            m_probability_or_probabilities_sizes (Union[float, list], optional):
                _description_. If provided, overrides the default m probabilities
                for the sizes specified. Defaults to None.
            m_probability_else (float, optional): If provided, overrides the
                default m probability for the 'anything else' level. Defaults to None.

        Returns:
            Comparison: A comparison that can be inclued in the Splink settings
                dictionary.
        """

        thresholds = ensure_is_iterable(length_thresholds)

        if m_probability_or_probabilities_sizes is None:
            m_probability_or_probabilities_sizes = [None] * len(thresholds)
        m_probabilities = ensure_is_iterable(m_probability_or_probabilities_sizes)

        comparison_levels = []
        comparison_levels.append(self._null_level(col_name))
        if include_exact_match_level:
            level = self._exact_match_level(
                col_name,
                term_frequency_adjustments=term_frequency_adjustments,
                m_probability=m_probability_exact_match,
            )
            comparison_levels.append(level)

        for length_thres, m_prob in zip(thresholds, m_probabilities):
            level = self._array_length_level(
                col_name,
                length_difference=length_thres,
                m_probability=m_prob,
            )
            comparison_levels.append(level)

        comparison_levels.append(
            self._else_level(m_probability=m_probability_else),
        )

        comparison_desc = ""
        if include_exact_match_level:
            comparison_desc += "Exact match vs. "

        thres_desc = ", ".join(thresholds)
        plural = "" if len(thresholds) == 1 else "s"
        comparison_desc += (
            f"Array length differences with threshold{plural} {thres_desc} vs. "
        )
        comparison_desc += "anything else"

        comparison_dict = {
            "comparison_description": comparison_desc,
            "comparison_levels": comparison_levels,
        }
        super().__init__(comparison_dict)
```

Crucially we needed to use `self._null_level`, `self._exact_match_level`, `self._else_level` which already exist,
but also the new `self._array_length_level` which relates to our new comparison level.
We will need to make sure that the dialect-specific comparisons which will actually be _used_ will have this property.

Each dialect has a comparison properties base, which stores information about all of the dialect-specific comparison levels used by all comparisons.
We will need to add our new level to this, which we referred to above in `ArrayLengthAtThresholdsBase` - for this example in [`splink.spark.spark_helpers.spark_comparison_imports`](https://github.com/moj-analytical-services/splink/blob/master/splink/spark/spark_helpers/spark_comparison_imports.py):
```python
from splink.spark.comparison_level_library import (
    exact_match_level,
    ...
    array_length_level,
)
...

class SparkComparisonProperties(SparkBase):
    @property
    def _exact_match_level(self):
        return exact_match_level
    ...
    @property
    def _array_length_level(self):
        return array_length_level
```

Any dialect-specific version of comparisons will inherit from this (where it learns about the dialect-specific comparison levels), and the comparison itself; in our case, in the same file:

```python
...
class array_length_at_thresholds(
    SparkComparisonProperties, ArrayLengthAtThresholds
):
    pass
```

This is now ready to import and be used, just as any other pre-existing comparisons.
