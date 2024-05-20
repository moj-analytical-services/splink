from splink.internals.validate_jsonschema import get_schema

schema = get_schema()

md = """
## Guide to Splink settings

This document enumerates all the settings and configuration options available when
developing your data linkage model.

You can find an interative settings editor [here](https://moj-analytical-services.github.io/splink/settingseditor/editor.html).

## Settings keys in the base setting dictionary

"""  # noqa E501


def add_doc_for_key(schema, key, md):
    this_key = schema[key]
    md += f"\n### {k}\n\n"
    md += f"{this_key['title']}\n\n"
    if "description" in this_key:
        md += f"{this_key['description']}\n\n"

    if "default" in this_key:
        md += f"**Default value**: `{this_key['default']}`\n\n"

    if "examples" in this_key:
        md += f"**Examples**: `{this_key['examples']}`\n\n"

    return md


root_schema = schema["properties"]
for k in root_schema.keys():
    md = add_doc_for_key(root_schema, k, md)

md += "## Settings keys nested within each member of `comparisons`"

comparisons_schema = schema["properties"]["comparisons"]["items"]["properties"]
for k in comparisons_schema.keys():
    md = add_doc_for_key(comparisons_schema, k, md)


md += "## Settings keys nested within each member of `comparison_levels`"

comparisons_levels_schema = schema["properties"]["comparisons"]["items"]["properties"][
    "comparison_levels"
]["items"]["properties"]
for k in comparisons_levels_schema.keys():
    md = add_doc_for_key(comparisons_levels_schema, k, md)

with open("docs/settings_dict_guide.md", "w") as f:
    f.write(md)
