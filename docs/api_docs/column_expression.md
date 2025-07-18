---
tags:
  - API
  - comparisons
  - blocking
---

# Column Expressions

In [comparisons](../topic_guides/comparisons/comparisons_and_comparison_levels.md), you may wish to consider expressions which are not simply columns of your input table.
For instance you may have a `forename` column in your data, but when comparing records you may wish to also use the values in this column transformed all to lowercase, or just the first three letters of the name, or perhaps both of these transformations taken together.

If it is feasible to do so, then it may be best to derive a new column containing the transformed data.
Particularly if it is an expensive calculation, or you wish to refer to it many times, deriving the column once on your input data may well be preferable, as it is cheaper than doing so directly in comparisons where each input record may need to be processed many times.
However, there may be situations where you don't wish to derive a new column, perhaps for large data where you have many such transformations, or when you are experimenting with different models.

This is where a `ColumnExpression` may be used. It represents some SQL expression, which may be a column, or some more complicated construct,
to which you can also apply zero or more transformations. These are lazily evaluated, and in particular will not be tied to a specific SQL dialect until they are put (via [settings](./settings_dict_guide.md) into a linker).
This can be particularly useful if you want to write code that can easily be switched between different backends.

??? warning "Term frequency adjustments"
    One caveat to using a `ColumnExpression` is that it cannot be combined with term frequency adjustments.
    Term frequency adjustments can only be computed on the raw values in a column prior to any function transforms.

    If you wish to use term frequencies with transformations of an input column, you must pre-compute a new column in your input data
    with the transforms applied, instead of a `ColumnExpression`.

```py
from splink import ColumnExpression

email_lowercase = ColumnExpression("email").lower()
dob_as_string = ColumnExpression("dob").cast_to_string()
surname_initial_lowercase = ColumnExpression("surname").substr(1, 1).lower()
entry_date = ColumnExpression("entry_date_str").try_parse_date(date_format="YYYY-MM-DD")
full_name_lowercase = ColumnExpression("first_name || ' ' || surname").lower()
```

You can use a `ColumnExpression` in most places where you might also use a simple column name, such as in a [library comparison](./comparison_library.md), a [library comparison level](./comparison_level_library.md), or in a [blocking rule](./blocking.md):

```py
from splink import block_on
import splink.comparison_library as cl
import splink.comparison_level_library as cll

full_name_lower_br = block_on(full_name_lowercase)

email_comparison = cl.DamerauLevenshteinAtThresholds(email_lowercase, distance_threshold_or_thresholds=[1, 3])
entry_date_comparison = cl.AbsoluteTimeDifferenceAtThresholds(
    entry_date,
    input_is_string=False,
    metrics=["day", "day"],
    thresholds=[1, 10],
)
name_comparison = cl.CustomComparison(
    comparison_levels=[
        cll.NullLevel(full_name_lowercase),
        cll.ExactMatch(full_name_lowercase),
        cll.ExactMatch("surname")
        cll.ExactMatch("first_name"),
        cll.ExactMatch(surname_initial_lowercase),
        cll.ElseLevel()
    ],
    output_column_name="name",
)
```


## `ColumnExpression`

::: splink.internals.column_expression.ColumnExpression
    handler: python
    options:
      show_root_heading: false
      heading_level: 3
      show_root_toc_entry: false
      show_source: false
      members_order: source
