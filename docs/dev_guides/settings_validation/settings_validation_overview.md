## Settings Validation

A common issue within Splink is users providing invalid settings dictionaries. To prevent this, the settings validator scans through a settings dictionary and provides user-friendly feedback on what needs to be fixed.

At a high level, this includes:

1. Assessing the structure of the settings dictionary. See the [Settings Schema Validation](#settings-schema-validation) section.
2. The contents of the settings dictionary. See the [Settings Vaildator](#settings-validator) section.

<hr>

## Settings Schema Validation

Our custom settings schema can be found within [settings_jsonschema.json](https://github.com/moj-analytical-services/splink/blob/master/splink/files/settings_jsonschema.json).

This is a json file, outlining the required data type, key and value(s) to be specified by the user while constructing their settings. Where values devivate from this specified schema, an error will be thrown.

[Schema validation](https://github.com/moj-analytical-services/splink/blob/master/splink/validate_jsonschema.py) is currently performed inside the [settings.py](https://github.com/moj-analytical-services/splink/blob/master/splink/settings.py#L44C17-L44C17) script.

You can modify the schema by manually editing the [json schema](https://github.com/moj-analytical-services/splink/blob/master/splink/files/settings_jsonschema.json).

Modifications can be used to (amongst other uses):

* Set or remove default values for schema keys.
* Set the required data type for a given key.
* Expand or refine previous titles and descriptions to help with clarity.

Any updates you wish to make to the schema should be discussed with the wider team, to ensure it won't break backwards compatability and makes sense as a design decision.

Detailed information on the arguments that can be suppled to the json schema can be found within the [json schema documentation](https://json-schema.org/learn/getting-started-step-by-step).

<hr>

## Settings Validator

The settings validation code currently resides in the [settings validation](https://github.com/moj-analytical-services/splink/tree/32e66db1c8c0bed54682daf9a6fea8ef4ed79ab4/splink/settings_validation) directory of Splink. This code is responsible for executing a secondary series of tests to determine whether all values within the settings dictionary will generate valid SQL.

Numerous inputs pass our initial schema checks before breaking other parts of the code base. These breaks are typically due to the construction of invalid SQL, that is then passed to the database engine, [commonly resulting in uninformative errors](https://github.com/moj-analytical-services/splink/issues/1362).

Frequently encountered problems include:

* Usage of invalid column names. For example, specifying a [`unique_id_column_name`](https://github.com/moj-analytical-services/splink/blob/settings_validation_docs/splink/files/settings_jsonschema.json#L61) that doesn't exist in the underlying dataframe(s). Such names satisfy the schema requirements as long as they are strings.
* Users not updating default values in the settings schema, even when these values are inappropriate for their provided input dataframes.
* Importing comparisons and blocking rules from incorrect sections of the codebase, or using an inappropriate data type (comparison level vs. comparison).
* Using Splink for an invalid form of linkage. See the [following dicsussion](https://github.com/moj-analytical-services/splink/issues/1362).

Currently, the [settings validation](https://github.com/moj-analytical-services/splink/tree/32e66db1c8c0bed54682daf9a6fea8ef4ed79ab4/splink/settings_validation) scripts are setup in a modular fashion, to allow each to inherit the checks it needs.

The folder is comprised of three scripts, each of which inspects the settings dictionary at different stages of its journey:

* [valid_types.py](https://github.com/moj-analytical-services/splink/blob/32e66db1c8c0bed54682daf9a6fea8ef4ed79ab4/splink/settings_validation/valid_types.py) - This script includes various miscellaneous checks for comparison levels, blocking rules, and linker objects. These checks are primarily performed within settings.py.
* [settings_validator.py](https://github.com/moj-analytical-services/splink/blob/32e66db1c8c0bed54682daf9a6fea8ef4ed79ab4/splink/settings_validation/settings_validator.py) - This script includes the core `SettingsValidator` class and contains a series of methods that retrieve information on fields within the user's settings dictionary that contain information on columns to be used in training and prediction. Additionally, it provides supplementary cleaning functions to assist in the removal of quotes, prefixes, and suffixes that may be present in a given column name.
* [column_lookups.py](https://github.com/moj-analytical-services/splink/blob/32e66db1c8c0bed54682daf9a6fea8ef4ed79ab4/splink/settings_validation/column_lookups.py) - This script contains helper functions that generate a series of log strings outlining invalid columns identified within your settings dictionary. It primarily consists of methods that run validation checks on either raw SQL or input columns and assesses their presence in **all** dataframes supplied by the user.

For information on expanding the range of checks available to the validator, see [Extending the Settings Validator](./extending_settings_validator.md).
