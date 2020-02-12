import pkg_resources
from jsonschema import validate, ValidationError
import json
import copy

# We probably don't want to read this json file every time we want to look in the dictionary
SCHEMA_CACHE = None

def _get_schema(setting_dict_should_be_complete=False):
    if SCHEMA_CACHE is None:
        with pkg_resources.resource_stream(__name__, "files/settings_jsonschema.json") as io:
            schema = json.load(io)
    else:
        schema = SCHEMA_CACHE

    if setting_dict_should_be_complete == False:
        return schema

    if setting_dict_should_be_complete:
        schema2 = copy.deepcopy(schema)
        schema2["required"] = ["proportion_of_matches",
                              "em_convergence",
                              "unique_id_column_name",
                              "comparison_columns"]

        schema2["comparison_columns"]["items"]["required"] = ["col_name",
                                                             "num_levels",
                                                             "case_expression",
                                                             "m_probabilities",
                                                             "u_probabilities"]
        return schema2


SCHEMA_CACHE = _get_schema()

def validate_settings(settings_dict, setting_dict_should_be_complete=False):
    schema = _get_schema()
    exception_raised = False
    try:
        validate(settings_dict, schema)
    except Exception as e:
        message = ("There is an error in your settings dictionary. "
                   "To quickly write a valid settings dictionary using autocompelte you might want to try "
                   "our online tool https://robinlinacre.com/simple_sparklink_settings_editor/, or you can use "
                   "the autocomplete features of VS Code - see example here"
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
        return schema["properties"]["comparison_columns"]["items"]["properties"][key]["default"]
    else:
        return schema["properties"][key]["default"]

