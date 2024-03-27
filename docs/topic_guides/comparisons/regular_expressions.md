
# Regular expressions (regex)

It can sometimes be useful to make comparisons based on substrings or parts of column values. For example, one approach to comparing postcodes is to consider their constituent components, e.g. area, district, etc (see [Featuring Engineering](../data_preparation/feature_engineering.md) for more details).

The `regex_extract` option enables users to do this by supplying a regular expression pattern that defines the substring upon which to evaluate a comparison. This option gives users a convenient means of comparing data within existing columns without needing to engineer new features from source data. `regex_extract` is available to all string comparators, as well as 'exact match' and 'columns reversed' comparisons and levels.

Further regex functionality is provided by the `valid_string_pattern` option. This option allows users to define a regular expression pattern that specifies a valid string format. Any column value that does not adhere to the given pattern will be treated as a null. This can be useful for enforcing a specific data format during record comparison without needing to revisit and standardised data again. The `valid_string_pattern` argument is available to the null level and can be used with any comparison that contains a null level, e.g. [`exact_match()`](../../comparison_library.md).

## Examples using `regex_extract`

### Exact match

Suppose you wish to make comparisons on a postcode column in your data, however only care about finding links between people who share the same area code (given by the first 1 to 2 letters of the postcode). The `regex_extract` option can be used within an exact match comparison to do this by passing it the regular expression pattern "`^[A-Z]{1,2}`":

=== "DuckDB"
    ```python
    import splink.duckdb.duckdb_comparison_library as cl

    pc_comparison = cl.exact_match("postcode", regex_extract="^[A-Z]{1,2}")
    ```

This gives a comparison with the following levels:

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

Below is an example set of record comparisons that could have been generated using `pc_comparison`. The part of the postcode actually being compared under the hood (the area code) is indicated in bold.

| person_id_l | person_id_r | postcode_l | postcode_r | comparison_level |
|-------------|-------------|------------|------------|------------------|
| 7           | 1           | **SE**1P 0NY   | **SE**1P 0NY   | exact match      |
| 5           | 1           | **SE**2 4UZ    | **SE**1P 0NY   | exact match      |
| 9           | 2           | **SW**14 7PQ   | **SW**3 9JG    | exact match      |
| 4           | 8           | **N**7 8RL     | **EC**2R 8AH   | else level       |
| 6           | 3           |            | **SE**2 4UZ    | null level       |


The [postcode comparison template](comparison_templates.ipynb) provides an example of a comparison which makes use of the `regex_extract` option across multiple exact match levels.

### Levenshtein

Using `regex_extract` in a Levenshtein comparison could be useful when comparing telephone numbers. For example, perhaps you only care about matches on dialling code but still want to account for potential typos in the data. (For more information on the different types of string comparators, see [String Comparators](comparators.md).)

This is achieved below by using `regex_extract="^[0-9]{1,4}"` within a Levenshtein comparison to restrict the comparison to only the first 3 to 4 digits of a telephone number:

=== "DuckDB"
    ```python
    import splink.duckdb.duckdb_comparison_library as cl

    tel_comparison = cl.levenshtein_at_thresholds("telephone", regex_extract="^[0-9]{1,4}")
    ```
which gives a comparison with the following levels:

??? note "Output"
    > Comparison 'Exact match vs. telephone within levenshtein thresholds 1, 2 vs. anything else' of "telephone".
    >
    > Similarity is assessed using the following ComparisonLevels:
    >
    >    - 'Null' with SQL rule: "telephone_l" IS NULL OR "telephone_r" IS NULL
    >    - 'Exact match' with SQL rule:
        regexp_extract("telephone_l", '^[0-9]{1,4}')
     =
        regexp_extract("telephone_r", '^[0-9]{1,4}')
    >    - 'Levenshtein <= 1' with SQL rule: levenshtein(
        regexp_extract("telephone_l", '^[0-9]{1,4}')
    ,
        regexp_extract("telephone_r", '^[0-9]{1,4}')
    ) <= 1
    >    - 'Levenshtein <= 2' with SQL rule: levenshtein(
        regexp_extract("telephone_l", '^[0-9]{1,4}')
    ,
        regexp_extract("telephone_r", '^[0-9]{1,4}')
    ) <= 2
    >    - 'All other comparisons' with SQL rule: ELSE

Here is an example set of record comparisons that could have been generated using `tel_comparison`. The part of the telephone number actually being compared under the hood (the dialling code) is highlighted in bold:

| person_id_l | person_id_r | telephone_l | telephone_r | comparison_level |
|-------------|-------------|-------------|-------------|------------------|
| 7           | 1           | **020** 5555 1234| **020** 4444 4573| exact match |
| 5           | 3           | **0161** 999 5678| **0160** 333 6521| levenshtein distance <= 1|
| 5           | 2           | **0161** 999 5678| **160** 221 2198| levenshtein distance <= 2|
| 4           | 1           | **0141** 777 9876| **020** 4444 4573 | else level|
| 6           | 7           |                  | **020** 5555 1234 | null level       |


### Jaro-Winkler

The `regex_extract` option can be useful in a Jaro-Winkler comparison of email addresses when the email domain is not considered important. (For more information on the different types of string comparators, see [String Comparators](comparators.md).)

Here `regex_extract` is used to extract everything before the '@' symbol in an email address, the username.

=== "DuckDB"
    ```python
    import splink.duckdb.duckdb_comparison_library as cl

    email_comparison = cl.jaro_winkler_at_thresholds("email", regex_extract="^[^@]+")
    ```

This gives a comparison with the following levels:

??? note "Output"
    > Comparison 'Exact match vs. Email within jaro_winkler_similarity thresholds 0.9, 0.7 vs. anything else' of "email".
    >
    > Similarity is assessed using the following ComparisonLevels:
    >
    > - 'Null' with SQL rule: "email_l" IS NULL OR "email_r" IS NULL
    > - 'Exact match' with SQL rule:
        regexp_extract("email_l", '^[^@]+')
     =
        regexp_extract("email_r", '^[^@]+')
    > - 'Jaro_winkler_similarity >= 0.9' with SQL rule: jaro_winkler_similarity(
        regexp_extract("email_l", '^[^@]+')
    ,
        regexp_extract("email_r", '^[^@]+')
    ) >= 0.9
    > - 'Jaro_winkler_similarity >= 0.7' with SQL rule: jaro_winkler_similarity(
        regexp_extract("email_l", '^[^@]+')
    ,
        regexp_extract("email_r", '^[^@]+')
    ) >= 0.7
    > - 'All other comparisons' with SQL rule: ELSE

Here is an example set of record comparisons that could have been generated using `email_comparison`. The part of the email address actually being compared under the hood is shown in bold:

| person_id_l | person_id_r | email_l | email_r | comparison_level               |
|-------------|-------------|---------|---------|--------------------------------|
| 7           | 1           |         |         | exact match                    |
| 5           | 1           |         |         | jaro-winkler similarity >= 0.9 |
| 9           | 2           |         |         | jaro-winkler similarity >= 0.7 |
| 4           | 8           |         |         | else level                     |
| 6           | 3           |         |         | null level                     |


## Example using `valid_string_pattern`

Recall that `valid_string_pattern` defines a regular expression pattern that if not matched will result in the column value being treated as a null. This can be a useful method to enforce a particular string format.

For example, you might want to insist that postcodes conform to the standard UK postcode format (see [here](https://ideal-postcodes.co.uk/guides/uk-postcode-format)), which in regex syntax corresponds to "^[A-Z]{1,2}[0-9][A-Z0-9]? [0-9][A-Z]{2}$"

=== "DuckDB"
    ```python
    import splink.duckdb.duckdb_comparison_library as cl

    pc_comparison = cl.exact_match("postcode", valid_string_pattern="^[A-Z]{1,2}[0-9][A-Z0-9]? [0-9][A-Z]{2}$")
    ```
which gives a comparison with the following levels:

??? note "Output"
    > Comparison 'Exact match vs. anything else' of "postcode".
    >
    > Similarity is assessed using the following ComparisonLevels:
    > - 'Null' with SQL rule:
        regexp_extract("postcode_l", '^[A-Z]{1,2}[0-9][A-Z0-9]? [0-9][A-Z]{2}$')
     IS NULL OR
        regexp_extract("postcode_r", '^[A-Z]{1,2}[0-9][A-Z0-9]? [0-9][A-Z]{2}$')
     IS NULL OR
        regexp_extract("postcode_l", '^[A-Z]{1,2}[0-9][A-Z0-9]? [0-9][A-Z]{2}$')
    =='' OR
        regexp_extract("postcode_r", '^[A-Z]{1,2}[0-9][A-Z0-9]? [0-9][A-Z]{2}$')
     ==''
    > - 'Exact match' with SQL rule: "postcode_l" = "postcode_r"
    > - 'All other comparisons' with SQL rule: ELSE

Here is an example set of record comparisons that could have been generated using this `pc_comparison` where postcodes which do not conform to the valid format are treated as null:

| person_id_l | person_id_r | postcode_l | postcode_r | comparison_level |
|-------------|-------------|------------|------------|------------------|
| 7           | 1           | SE1P 0NY   | SE1P 0NY   | exact match      |
| 9           | 2           | S 7PQ      | SW3 9JG    | null level       |
| 6           | 3           |            | SE2 4UZ    | null level       |
| 5           | 1           | SE2 4UZ    | SE1P 0NY   | else level       |

## Additional info

Regular expressions containing “\” (the python escape character) are not compatible with Splink’s Spark linker so please consider using alternative syntax, for example replacing “\d” with “[0-9]”.

Different regex patterns can achieve the same result but with more or less efficiency. You might want to consider optimising your regular expressions to improve performance (see [here](https://www.loggly.com/blog/regexes-the-bad-better-best/), for example).
