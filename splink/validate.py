import pkg_resources
from functools import lru_cache

from jsonschema import validate, ValidationError

import json
import copy

from typeguard import typechecked


@lru_cache()
def _get_schema(setting_dict_should_be_complete=False):
    with pkg_resources.resource_stream(
        __name__, "files/settings_jsonschema.json"
    ) as io:
        schema = json.load(io)

    if not setting_dict_should_be_complete:
        return schema

    if setting_dict_should_be_complete:
        schema2 = copy.deepcopy(schema)
        schema2["required"] = [
            "proportion_of_matches",
            "em_convergence",
            "unique_id_column_name",
            "comparison_columns",
        ]

        schema2["comparison_columns"]["items"]["required"] = [
            "col_name",
            "num_levels",
            "case_expression",
            "m_probabilities",
            "u_probabilities",
        ]
        return schema2


@typechecked
def validate_settings(settings_dict: dict):
    """Validate a splink settings object against its jsonschema

    Args:
        settings_dict (dict): A splink settings dictionary

    Raises:
        ValidationError: [description]

    Returns:
        [type]: [description]
    """

    schema = _get_schema()
    exception_raised = False
    try:
        validate(settings_dict, schema)
    except Exception as e:
        message = (
            "There is an error in your settings dictionary. "
            "To quickly write a valid settings dictionary using autocompelte you might want to try "
            "our online tool https://moj-analytical-services.github.io/splink_settings_editor/ or you can use "
            "the autocomplete features of VS Code - just copy and paste code from the following gist "
            "into VS Code, setting language mode to json, or having saved the file as a .json file\n"
            "https://gist.github.com/RobinL/cfe1152dbd33ae26e05a43d9a0ec85b9"
            "\n\n"
            "The details of the error are as follows:"
            "\n"
        )
        message = message + str(e)
        exception_raised = True

    if exception_raised:
        raise ValidationError(message)


def _get_default_value(key, is_column_setting):
    schema = _get_schema()

    if is_column_setting:
        return schema["properties"]["comparison_columns"]["items"]["properties"][key][
            "default"
        ]
    else:
        return schema["properties"][key]["default"]


def validate_input_datasets(df, completed_settings_obj):
    """Check that the input datasets contain the columns needed to run the model

    Args:
        df (DataFrame): Dataframe to be used in splink - either the original df, or the concatenation of the input list of dfs
        completed_settings_obj (Settings): Settings object
    """
    cols_present = set(df.columns)

    cols_needed = set()

    s = completed_settings_obj
    cols_needed.add(s["unique_id_column_name"])
    if completed_settings_obj["link_type"] != "dedupe_only":
        cols_needed.add(s["source_dataset_column_name"])

    for cc in s.comparison_columns_list:
        cols_needed.update(cc.input_cols_used)

    required_cols_not_present = cols_needed - cols_present
    if len(required_cols_not_present):
        raise ValueError(
            f"Cannot find columns {required_cols_not_present} "
            "For Splink to be work with the settings provided, "
            "your input dataframes must  include the following columns "
            f"{cols_needed}"
        )


def validate_link_type(df_or_dfs, settings):
    if type(df_or_dfs) == list:
        if "link_type" in settings:
            if settings["link_type"] == "dedupe_only":
                raise ValueError(
                    "If you provide a list of dfs, link_type must be "
                    "link_only or link_and_dedupe, not dedupe_only"
                )
