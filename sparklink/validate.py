import pkg_resources
from jsonschema import validate, ValidationError
import json

def get_schema():
    with pkg_resources.resource_stream(__name__, "files/settings_jsonschema.json") as io:
        schema = json.load(io)
    return schema

def validate_settings(settings_dict):
    schema = get_schema()
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



