
# Regular expressions (regex)

It can sometimes be useful to make comparisons based on substrings of column values. For example, a sensible approach to comparing postcodes can be to consider their constituent components (e.g. area, district, etc) by extracting them as substrings of the full postcode (see [Featuring Engineering](feature_engineering.md) for more details).

The `regex_extract` option enables this by allowing users to provide a regular expression pattern on which to evaluate a match. The option is available to all string comparators, as well as exact match and columns reversed comparisons and levels. `regex_extract` gives users a way of comparing...something... of existing columns without having to go back to source data and engineer new features.

Further regex functionality is provided via the `valid_string_regex` option, available to the null comparison and level. It allows users to provide a regular expression pattern defining a valid string format that, if not matched, will result in the column being treated as a null. This can be useful if you wish to enforce a data strong format witout having to re-standardize data...

For more information on writing efficient/optimised regex please see...[link]

...The examples using `regex_extract` and `valid_string_regex` given below operate on the following dataframe:...

## Examples using `regex_extract`

### Exact match

1. Motivate the example...

Supppose you wish to make comparisons on a postcode column in your data, however only care about finding links between people who share the same area code (given by the first 1 to 2 letters of the postcode). We can use an exact match comparison with `regex_extract` to do this by passing the regular expression pattern "`^[A-Z]{1,2}`" to the argument.

[select, pick out]

2. Give code on how to use the regex_extract in comparison. Writeout output with gamma values given.

=== "DuckDB"
    ```python
    import splink.duckdb.duckdb_comparison_library as cl

    pc_comparison = cl.exact_match("postcode", regex_extract="^[A-Z]{1,2}")
    ```

which gives a comparison with the following levels

??? note "Output"
    > Comparison 'Exact match vs. anything else' of "postcode".
    >
    > Similarity is assessed using the following ComparisonLevels:
    >
    >    - 'Null' with SQL rule: "postcode_l" IS NULL OR "postcode_r" IS NULL
    >    - 'Exact match' with SQL rule: 
        regexp_extract("postcode_l", '^[A-Z]{1,2}')
     = 
        regexp_extract("postcode_r", '^[A-Z]{1,2}')        
    >    - 'All other comparisons' with SQL rule: ELSE


3. Table showing an example of edges - postcodes side by side, with the bit being compared highlighted in bold and the gamma level, plus translation to exact etc
(produced from a dataframe of the form...don't worry about this bit for now)

don't make all the pngs until checked that it's a good format

Show what is extracted, the result of extracting and the comparison being made under the hood- can you show the intermediate col from a table?


### Levenshtein



### Jaro-Winkler

1. Motivate using regex extract on a name (or other) comparison

2. code example

=== "DuckDB"
    ```python
    import splink.duckdb.duckdb_comparison_library as cl

    name_comparison = cl.jaro_winkler("name", regex_extract="")
    ```

which gives a comparison with the following levels


## Example using `valid_regex_string`



