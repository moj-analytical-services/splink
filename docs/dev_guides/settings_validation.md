## Settings Schema Validation

[Schema validation](https://github.com/moj-analytical-services/splink/blob/master/splink/validate_jsonschema.py) is currently performed inside the [settings.py](https://github.com/moj-analytical-services/splink/blob/master/splink/settings.py#L44C17-L44C17) script.

This assesses the user's settings dictionary against our custom [settings schema](https://github.com/moj-analytical-services/splink/blob/master/splink/files/settings_jsonschema.json). Where the data type, key or entered values devivate from what is specified in the schema, an error will be thrown.

You can modify the schema by editing the json file [here](https://github.com/moj-analytical-services/splink/blob/master/splink/files/settings_jsonschema.json).

Detailed information on the arguments that can be suppled is available within the [json schema documentation](https://json-schema.org/learn/getting-started-step-by-step).

<hr>

## Settings Validator

In addition to the Settings Schema Validation, we have implemented checks to evaluate the validity of a user's settings in the context of the provided input dataframe(s) and linker type.

??? note "The current validation checks performed include:"
    * Verifying that the user's blocking rules and comparison levels have been [imported from the correct library](https://github.com/moj-analytical-services/splink/pull/1579) and contain sufficient information for use in a Splink model.
    * [Performing column lookups]((https://github.com/moj-analytical-services/splink/blob/master/splink/settings_validation/column_lookups.py)), confirming that columns specified in the user's settings dictionary exist within **all** of the user's input dataframes.
    * Conducting various miscellaneous checks that generate more informative error messages for the user, should they be using Splink in an unintended manner.

All components related to our settings checks are currently located in the [settings validation](https://github.com/moj-analytical-services/splink/tree/32e66db1c8c0bed54682daf9a6fea8ef4ed79ab4/splink/settings_validation) within Splink.

This folder is comprised of three scripts, each of which inspects the settings dictionary at different stages of its journey:

* [valid_types.py](https://github.com/moj-analytical-services/splink/blob/32e66db1c8c0bed54682daf9a6fea8ef4ed79ab4/splink/settings_validation/valid_types.py) - This script includes various miscellaneous checks for comparison levels, blocking rules, and linker objects. These checks are primarily performed within settings.py.
* [settings_validator.py](https://github.com/moj-analytical-services/splink/blob/32e66db1c8c0bed54682daf9a6fea8ef4ed79ab4/splink/settings_validation/settings_validator.py) - This script includes the core `SettingsValidator` class and contains a series of methods that retrieve information on fields within the user's settings dictionary that contain information on columns to be used in training and prediction. Additionally, it provides supplementary cleaning functions to assist in the removal of quotes, prefixes, and suffixes that may be present in a given column name.
* [column_lookups.py](https://github.com/moj-analytical-services/splink/blob/32e66db1c8c0bed54682daf9a6fea8ef4ed79ab4/splink/settings_validation/column_lookups.py) - This script contains helper functions that generate a series of log strings outlining invalid columns identified within your settings dictionary. It primarily consists of methods that run validation checks on either raw SQL or input columns and assesses their presence in **all** dataframes supplied by the user.

<hr>

## Extending the Settings Validator

Before adding any code, it's initially import to identify and determine whether the checks you wish to add fall into any of the broad buckets outlined in the scripts above.

If you intend to introduce checks that are distinct from those currently in place, it is advisable to create a new script within `splink/settings_validation`.

The current options you may wish to extend are as follows:

### Logging Errors

Logging multiple errrors sequentially without disrupting the program, is a common issue. To enable the logging of multiple errors in a singular check, or across multiple checks, an [`ErrorLogger`](https://github.com/moj-analytical-services/splink/blob/settings_validation_refactor_and_improved_logging/splink/exceptions.py#L34) class is available for use.

The `ErrorLogger` operates in a similar way to working with a list, allowing you to add additional errors using the `append` method. Once you've logged all of your errors, you can raise them with the `raise_and_log_all_errors` method.

??? note "`ErrorLogger` in practice"
    ```py
    from splink.exceptions import ErrorLogger

    # Create an error logger instance
    e = ErrorLogger()

    # Log your errors
    e.append(SyntaxError("The syntax is wrong"))
    e.append(NameError("Invalid name entered"))

    # Raise your errors
    e.raise_and_log_all_errors()
    ```


### Expanding our Miscellaneous Checks

Miscellaneous checks should typically be added as standalone functions. These functions can then be integrated into the linker's startup process for validation.

In most cases, you have more flexibility in how you structure your solutions. You can place the checks in a script that corresponds to the specific checks being performed, or, if one doesn't already exist, create a new script with a descriptive name.

A prime example of a miscellaneous check is [`validate_dialect`](https://github.com/moj-analytical-services/splink/blob/master/splink/settings_validation/valid_types.py#L31), which assesses whether the settings dialect aligns with the linker's dialect.


### Additional Comparison and Blocking Rule Checks

If your checks pertain to comparisons or blocking rules, most of these checks are currently implemented within the [valid_types.py](https://github.com/moj-analytical-services/splink/blob/32e66db1c8c0bed54682daf9a6fea8ef4ed79ab4/splink/settings_validation/valid_types.py) script.

Currently, comparison and blocking rule checks are organised in a modular format.

To expand the current suite of tests, you should:

1. Create a function to inspect the presence of the error you're evaluating.
2. Define an error message that you intend to add to the `ErrorLogger` class.
3. Integrate these elements into either the `validate_comparison_levels` function or a similar one, which appends any detected errors.
4. Finally, work out where this function should live in the setup process of the linker object.


### Additional Column Checks

Should you need to include extra checks to assess the validity of columns supplied by a user, your primary focus should be on the [column_lookups.py](https://github.com/moj-analytical-services/splink/blob/master/splink/settings_validation/column_lookups.py) script.

There are currently three classes employed to construct the current log strings. These can be extended to perform additional column checks.

??? note "`InvalidCols`"
    `InvalidCols` is a NamedTuple, used to construct the bulk of our log strings. This accepts a list of columns and the type of error, producing a complete log string when requested.

    In practice, this is used as follows:
    ```py
    # Store the invalid columns and why they're invalid
    my_invalid_cols = InvalidCols("invalid_cols", ["first_col", "second_col"])
    # Construct the corresponding log string
    my_invalid_cols.construct_log_string()
    ```

??? note "`InvalidColValidator`"
    `InvalidColValidator` houses a series of validation checks to evaluate whether the column(s) contained within either a SQL string or a user's raw input string, are present within the underlying dataframes.

    To achieve this, it employs a range of cleaning functions to standardise our column inputs and conducts a series of checks on these cleaned columns. It utilises `InvalidCols` tuples to log any identified invalid columns.

    It inherits from our the `SettingsValidator` class.

??? note "`InvalidColumnsLogger`"
    The principal logging class for our invalid column checks.

    This class primarily calls our builder functions outlined in `InvalidColValidator`, constructing a series of log strings for output to both the console and the user's log file (if it exists).

To extend the column checks, you simply need to add an additional validation method to the `InvalidColValidator` class. The implementation of this will vary depending on whether you merely need to assess the validity of a single column or columns within a SQL statement.

#### Single Column Checks
To assess single columns, the `validate_settings_column` should typically be used. This takes in a `setting_id` (analogous to the title you want to give your log string) and a list of columns to be checked.

Should you need more control, the setup for checking a single column by hand is simple.

To perform checks on a single column, you need to:

* Clean your input text
* Run a lookup of the clean text against the raw input dataframe(s) columns.
* Construct log string (typically handled by `InvalidCols`)

Cleaning and validation can be performed with a single method called - `clean_and_return_missing_columns`.

See the `validate_uid` and `validate_columns_to_retain` methods for examples of this process in practice.

#### Raw SQL Checks
For raw SQL statements, you should be able to make use of the `validate_columns_in_sql_strings` method. This takes in a list of SQL strings and spits out a list of `InvalidCols` tuples, depending on the checks you ask it to perform.

Should you need more control, the process is similar to that of the single column case, just with an additional parsing step.

Parsing is handled by `parse_columns_in_sql`, which is found within `splink.parse_sql`. This will spit out a list of column names that were identified.

Note that this is handled by SQLglot, meaning that it's not always 100% accurate. For our purposes though, its flexibility is unparalleled and allows us to more easily and efficiently extract column names.

Once parsed, you can again run a series of lookups against your input dataframes in a loop. See **Single Column Checks** for more info.

As there are limited constructors present within , it should be incredibly lightweight to create a new class to be used ...
