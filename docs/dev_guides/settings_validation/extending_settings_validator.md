## Expanding the Settings Validator

If a validation check is currently missing, you might want to expand the existing validation codebase.

Before adding any code, it's essential to determine whether the checks you want to include fit into any of the general validation categories already in place.

In summary, the following validation checks are currently carried out:

* Verifying that the user's blocking rules and comparison levels have been [imported from the correct library](https://github.com/moj-analytical-services/splink/pull/1579) and contain sufficient information for Splink model usage.
* [Performing column lookups](https://github.com/moj-analytical-services/splink/blob/master/splink/settings_validation/column_lookups.py) to ensure that columns specified in the user's settings dictionary exist within **all** of the user's input dataframes.
* Various miscellaneous checks designed to generate more informative error messages for the user if they happen to employ Splink in an unintended manner.

If you plan to introduce checks that differ from those currently in place, it's advisable to create a new script within `splink/settings_validation`.

<hr>

## Splink Exceptions and Warnings

While working on extending the settings validation tools suite, it's important to consider how we notify users when they've included invalid settings or features.

Exception handling and warnings should be integrated into your validation functions to either halt the program or inform the user when errors occur, raising informative error messages as needed.

### Warnings in Splink

Warnings should be employed when you want to alert the user that an included setting might lead to unintended consequences, allowing the user to decide if it warrants further action.

This could be applicable in scenarios such as:

* Parsing SQL where the potential for failure or incorrect column parsing exists.
* Situations where the user is better positioned to determine whether the issue should be treated as an error, like when dealing with exceptionally high values for [probability_two_random_records_match](https://github.com/moj-analytical-services/splink/blob/master/splink/files/settings_jsonschema.json#L29).

Implementing warnings is straightforward and involves creating a logger instance within your script, followed by a warning call.

??? note "Warnings in practice:"
    ```py
    import logging
    logger = logging.getLogger(__name__)

    logger.warning("My warning message")
    ```

    Which will print:

    > `My warning message`

    to both the console and your log file.

### Splink Exceptions

Exceptions should be raised when you want the program to halt due to an unequivocal error.

In addition to the built-in exception types, such as [SyntaxError](https://docs.python.org/3/library/exceptions.html#SyntaxError), we have several Splink-specific exceptions available for use.

These exceptions serve to raise issues specific to Splink or to customize exception behavior. For instance, you can specify a message prefix by modifying the constructor of an exception, as exemplified in the [ComparisonSettingsException](https://github.com/moj-analytical-services/splink/blob/f7c155c27ccf3c906c92180411b527a4cfd1111b/splink/exceptions.py#L14).

It's crucial to also consider how to inform the user that such behavior is not permitted. For guidelines on crafting effective error messages, refer to [How to Write Good Error Messages](https://uxplanet.org/how-to-write-good-error-messages-858e4551cd4).

For a comprehensive list of exceptions native to Splink, visit [the exceptions.py script](https://github.com/moj-analytical-services/splink/blob/master/splink/exceptions.py).

#### Raising Multiple Exceptions

Raising multiple errors sequentially without disrupting the program, is a feature we commonly wish to implement across the validation steps.

In numerous instances, it makes sense to wait until all checks have been performed before raising exceptions captured to the user in one go.

To enable the logging of multiple errors in a singular check, or across multiple checks, an [`ErrorLogger`](https://github.com/moj-analytical-services/splink/blob/settings_validation_refactor_and_improved_logging/splink/exceptions.py#L34) class is available for use.

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

    ![](https://raw.githubusercontent.com/moj-analytical-services/splink/master/docs/img/settings_validation/error_logger.png)

<hr>

## Expanding our Miscellaneous Checks

Miscellaneous checks should typically be added as standalone functions. These functions can then be integrated into the linker's startup process for validation.

In most cases, you have more flexibility in how you structure your solutions. You can place the checks in a script that corresponds to the specific checks being performed, or, if one doesn't already exist, create a new script with a descriptive name.

A prime example of a miscellaneous check is [`validate_dialect`](https://github.com/moj-analytical-services/splink/blob/master/splink/settings_validation/valid_types.py#L31), which assesses whether the settings dialect aligns with the linker's dialect.

<hr>

## Additional Comparison and Blocking Rule Checks

If your checks pertain to comparisons or blocking rules, most of these checks are currently implemented within the [valid_types.py](https://github.com/moj-analytical-services/splink/blob/32e66db1c8c0bed54682daf9a6fea8ef4ed79ab4/splink/settings_validation/valid_types.py) script.

Currently, comparison and blocking rule checks are organised in a modular format.

To expand the current suite of tests, you should:

1. Create a function to inspect the presence of the error you're evaluating.
2. Define an error message that you intend to add to the `ErrorLogger` class.
3. Integrate these elements into either the [`validate_comparison_levels`](https://github.com/moj-analytical-services/splink/blob/32e66db1c8c0bed54682daf9a6fea8ef4ed79ab4/splink/settings_validation/valid_types.py#L43) function (or something similar), which appends any detected errors to an `ErrorLogger`.
4. Finally, work out where this function should live in the setup process of the linker object. Typically, you should look to add these checks before any processing of the settings dictionary is performed.

The above steps are set to change as we are looking to refactor our settings object.

<hr>

## Checking that columns exist

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


To extend the column checks, you simply need to add an additional validation method to the [`InvalidColValidator`](https://github.com/moj-analytical-services/splink/blob/master/splink/settings_validation/column_lookups.py#L15) class, followed by an extension of the [`InvalidColumnsLogger`](https://github.com/moj-analytical-services/splink/blob/master/splink/settings_validation/column_lookups.py#L164).

### A Practical Example of a Column Check

For an example of column checks in practice, see [`validate_uid`](https://github.com/moj-analytical-services/splink/blob/master/splink/settings_validation/column_lookups.py#L195).

Here, we call `validate_settings_column`, checking whether the unique ID column submitted by the user is valid. The output of this call yields either an `InvalidCols` tuple, or `None`.

From there, we can use the built-in log constructor [`construct_generic_settings_log_string`](https://github.com/moj-analytical-services/splink/blob/master/splink/settings_validation/column_lookups.py#L329C27-L329C27) to construct and print the required logs. Where the output above was `None`, nothing is logged.

If your checks aren't part of the initial settings check (say you want to assess additional columns found in blocking rules supplied at a later stage by the user), you should add a new method to `InvalidColumnsLogger`, similar in functionality to [`construct_output_logs`](https://github.com/moj-analytical-services/splink/blob/master/splink/settings_validation/column_lookups.py#L319).

However, it is worth noting that not all checks are performed on a simple string columns. Where you require checks to be performed on SQL strings, there's an additional step required, outlined below.

### Single Column Checks

To review single columns, [`validate_settings_column`](https://github.com/moj-analytical-services/splink/blob/master/splink/settings_validation/column_lookups.py#L144) should be used. This takes in a `setting_id` (analogous to the title you want to give your log string) and a list of columns to be checked.

A working example of this in practice can be found in the section above.

### Checking Columns in SQL statements

For raw SQL statements, you should make use of the [`validate_columns_in_sql_strings`](https://github.com/moj-analytical-services/splink/blob/master/splink/settings_validation/column_lookups.py#L102) method.

This takes in a list of SQL strings and spits out a list of `InvalidCols` tuples, depending on the checks you ask it to perform.

Should you need more control, the process is similar to that of the single column case, just with an additional parsing step.

Parsing is handled by [`parse_columns_in_sql`](https://github.com/moj-analytical-services/splink/blob/master/splink/parse_sql.py#L45). This will spit out a list of column names that were identified by sqlglot.

> Note that as this is handled by SQLglot, it's not always 100% accurate. For our purposes though, its flexibility is unparalleled and allows us to more easily and efficiently extract column names.

Once your columns have been parsed, you can again run a series of lookups against your input dataframe(s). This is identical to the steps outlined in the **Single Column Checks** section.

You may also wish to perform additional checks on the columns, to assess whether they contain valid prefixes, suffixes or some other quality of the column.

Additional checks can be passed to [`validate_columns_in_sql_strings`](https://github.com/moj-analytical-services/splink/blob/master/splink/settings_validation/column_lookups.py#L102) and should be specified as methods in the `InvalidColValidator` class.

See [validate_blocking_rules](https://github.com/moj-analytical-services/splink/blob/master/splink/settings_validation/column_lookups.py#L209) for a practical example where we loop through each blocking rule, parse it and then assess whether it:

1. Contains a valid list of columns
2. Each column contains a valid table prefix.
