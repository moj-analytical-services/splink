## Settings Validation

A common problem within Splink comes from users providing invalid settings dictionaries. To prevent this, we've built a settings validator to scan through a given settings dictionary and provide user-friendly feedback on what needs to be fixed.

At a high level, this includes:

1. Assessing the structure of the settings dictionary. See the [Settings Schema Validation](#settings-schema-validation) section.
2. The contents of the settings dictionary. See the [Settings Validator](#settings-validator) section.

<hr>

## Settings Schema Validation

Our custom settings schema can be found within [`settings_jsonschema.json`](https://github.com/moj-analytical-services/splink/blob/master/splink/files/settings_jsonschema.json).

This is a json file, outlining the required data type, key and value(s) to be specified by the user while constructing their settings. Where values deviate from this specified schema, an error will be thrown.

[Schema validation](https://github.com/moj-analytical-services/splink/blob/master/splink/validate_jsonschema.py) is currently performed inside the [settings.py](https://github.com/moj-analytical-services/splink/blob/master/splink/settings.py#L44C17-L44C17) script.

You can modify the schema by manually editing the [json schema](https://github.com/moj-analytical-services/splink/blob/master/splink/files/settings_jsonschema.json).

Modifications can be used to (amongst other uses):

- Set or remove default values for schema keys.
- Set the required data type for a given key.
- Expand or refine previous titles and descriptions to help with clarity.

Any updates you wish to make to the schema should be discussed with the wider team, to ensure it won't break backwards compatibility and makes sense as a design decision.

Detailed information on the arguments that can be supplied to the json schema can be found within the [json schema documentation](https://json-schema.org/learn/getting-started-step-by-step).

<hr>

## Settings Validator

As long as an input is of the correct data type, it will pass our initial schema checks. This can then mean that user inputs that would generate invalid SQL can slip through and are then often caught by the database engine, [commonly resulting in uninformative errors](https://github.com/moj-analytical-services/splink/issues/1362). This can result in uninformative and confusing errors that the user is unsure of how to resolve.

The settings validation code (found within the [settings validation](https://github.com/moj-analytical-services/splink/tree/master/splink/settings_validation) directory of Splink) is another layer of validation, executing a series of checks to determine whether values in the user's settings dictionary will generate invalid SQL.

Frequently encountered problems include:

- **Invalid column names**. For example, specifying a [`unique_id_column_name`](https://github.com/moj-analytical-services/splink/blob/settings_validation_docs/splink/files/settings_jsonschema.json#L61) that doesn't exist in the underlying dataframe(s). Such names satisfy the schema requirements as long as they are strings.
- **Using the settings dictionary's default values**
- **Importing comparisons and blocking rules for the wrong dialect**.
- **Using an inappropriate custom data types** - (comparison level vs. comparison within our comparisons).
- **Using Splink for an invalid form of linkage** - See the [following discussion](https://github.com/moj-analytical-services/splink/issues/1362).


All code relating to [settings validation](https://github.com/moj-analytical-services/splink/tree/32e66db1c8c0bed54682daf9a6fea8ef4ed79ab4/splink/settings_validation) can be found within one of the following scripts:

- [valid_types.py](https://github.com/moj-analytical-services/splink/blob/master/splink/settings_validation/valid_types.py) - This script includes various miscellaneous checks for comparison levels, blocking rules, and linker objects. These checks are primarily performed within settings.py.
- [settings_column_cleaner.py](https://github.com/moj-analytical-services/splink/blob/master/splink/settings_validation/settings_column_cleaner.py) - Includes a set of functions for cleaning and extracting data, designed to sanitise user inputs in the settings dictionary and retrieve necessary SQL or column identifiers.
- [log_invalid_columns.py](https://github.com/moj-analytical-services/splink/blob/master/splink/settings_validation/log_invalid_columns.py) - Pulls the information extracted in [settings_column_cleaner.py](https://github.com/moj-analytical-services/splink/blob/master/splink/settings_validation/settings_column_cleaner.py) and generates any log strings outlining invalid columns or SQL identified within the settings dictionary. Any generated error logs are reported to the user when initialising a linker object at the `INFO` level.
- [settings_validation_log_strings.py](https://github.com/moj-analytical-services/splink/blob/master/splink/settings_validation/settings_validation_log_strings.py) - a home for any error messages or logs generated by the settings validator.

For information on expanding the range of checks available to the validator, see [Extending the Settings Validator](./extending_settings_validator.md).
