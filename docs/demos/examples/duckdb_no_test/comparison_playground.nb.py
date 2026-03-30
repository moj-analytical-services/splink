# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.18.1
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% tags=["hide_input"]
# Uncomment and run this cell if you're running in Google Colab.
# # !pip install ipywidgets
# # !pip install splink
# # !jupyter nbextension enable --py widgetsnbextension

# %% [markdown]
# ### Comparison values
#
# Run the following cell to get an interactive interface for trying out comparisons from the `comparison_library`.
#
# The interface uses the default arguments for each comparison, and can only be used for comparisons that refer to a single column
#
# In the following cells, you can get similar results for custom comparisons with an arbitrary number of input columns.

# %%
import ipywidgets as widgets
from IPython.display import Markdown, display

import splink.comparison_library as cl
from splink import DuckDBAPI
from splink.internals.testing import is_in_level


def report_comparison_levels(comparison, values_dict, column_name):
    db_api = DuckDBAPI()
    levels = comparison.create_comparison_levels()
    comparison_dict = comparison.get_comparison("duckdb").as_dict()

    table_rows = []
    table_rows.append("| Match | Level | Description | SQL Condition |")
    table_rows.append("|-------|-------|-------------|---------------|")

    total_levels = len(levels)
    matched_level = None
    for i, level in enumerate(levels):
        level_number = total_levels - i - 1
        label = level.create_label_for_charts()
        sql_condition = comparison_dict['comparison_levels'][i]['sql_condition']

        is_match = is_in_level(level, values_dict, db_api)

        if is_match and matched_level is None:
            matched_level = level_number
            match_indicator = "✓"
            row = f"| {match_indicator} | **{level_number}** | **{label}** | `{sql_condition}` |"
        else:
            match_indicator = ""
            row = f"| {match_indicator} | {level_number} | {label} | `{sql_condition}` |"

        table_rows.append(row)

    markdown_output = "\n".join(table_rows)
    return markdown_output

def create_comparison_playground(column_name):

    comparison_types = [
        'ExactMatch', 'LevenshteinAtThresholds', 'JaroAtThresholds',
        'JaroWinklerAtThresholds', 'DamerauLevenshteinAtThresholds',
        'JaccardAtThresholds',
        'AbsoluteDateDifferenceAtThresholds',
        'ArrayIntersectAtSizes', 'DateOfBirthComparison',
         'EmailComparison',
        'NameComparison', 'PostcodeComparison'
    ]

    default_values = {
        'ExactMatch': ('john', 'jon'),
        'LevenshteinAtThresholds': ('smith', 'smyth'),
        'JaroAtThresholds': ('martha', 'matha'),
        'JaroWinklerAtThresholds': ('williams', 'willaims'),
        'DamerauLevenshteinAtThresholds': ('receive', 'recieve'),
        'CosineSimilarityAtThresholds': ('data science', 'science data'),
        'JaccardAtThresholds': ('0123456789', '012345678'),
        'AbsoluteDateDifferenceAtThresholds': ('2023-01-01', '2023-01-15'),
        'ArrayIntersectAtSizes': ('apple,banana,cherry', 'banana,cherry,date'),
        'DateOfBirthComparison': ('1990-05-15', '1990-05-16'),
        'EmailComparison': ('john.doe@example.com', 'john.doe@other.com'),
        'NameComparison': ('Elizabeth', 'Elisabeth'),
        'PostcodeComparison': ('SW1A 1AA', 'SW1A 1AB')
    }

    docstrings = {}
    for comp_type in comparison_types:
        class_obj = getattr(cl, comp_type)
        init_doc = getattr(class_obj.__init__, '__doc__', None)
        docstrings[comp_type] = init_doc if init_doc else class_obj.__doc__


    def get_comparison(comp_type):
        if comp_type in ['DateOfBirthComparison']:

            return getattr(cl, comp_type)(column_name, input_is_string=True)
        if comp_type == 'AbsoluteDateDifferenceAtThresholds':
            return getattr(cl, comp_type)(column_name, input_is_string=True, metrics=["day", "month"], thresholds=[1, 1])
        elif comp_type in ['EmailComparison', 'ForenameSurnameComparison', 'NameComparison', 'PostcodeComparison', 'ArrayIntersectAtSizes']:
            return getattr(cl, comp_type)(column_name)
        else:
            return getattr(cl, comp_type)(column_name)

    def run_comparison(change):
        left_value = left_input.value if left_input.value != "" else None
        right_value = right_input.value if right_input.value != "" else None
        comparison = get_comparison(comparison_select.value)

        if comparison_select.value == 'ArrayIntersectAtSizes':
            left_value = left_value.split(',') if left_value else None
            right_value = right_value.split(',') if right_value else None

        values_dict = {f"{column_name}_l": left_value, f"{column_name}_r": right_value}

        output.clear_output()
        markdown_output = report_comparison_levels(comparison, values_dict, column_name)
        with output:
            display(Markdown("### Comparison levels:"))
            display(Markdown(markdown_output))

            docstring = docstrings.get(comparison_select.value, "No docstring available")
            processed_docstring = "\n".join(line.strip() for line in docstring.split("\n"))
            display(Markdown("### Comparison Function Docstring:"))
            display(Markdown(processed_docstring))


        # Store the markdown output for later use
        playground.markdown_output = markdown_output



    def on_comparison_change(change):
        new_value = change['new']
        left_value, right_value = default_values.get(new_value, ('', ''))

        # Temporarily unobserve the input widgets
        left_input.unobserve(run_comparison, names='value')
        right_input.unobserve(run_comparison, names='value')

        # Update the values
        left_input.value = left_value
        right_input.value = right_value

        # Re-observe the input widgets
        left_input.observe(run_comparison, names='value')
        right_input.observe(run_comparison, names='value')

        # Run the comparison once after updating both inputs
        run_comparison(None)


    comparison_select = widgets.Dropdown(
        options=comparison_types,
        value='ExactMatch',
        description='Comparison:',
    )
    left_input = widgets.Text(description=f"{column_name} Left:", value=default_values['ExactMatch'][0])
    right_input = widgets.Text(description=f"{column_name} Right:", value=default_values['ExactMatch'][1])
    output = widgets.Output()

    comparison_select.observe(on_comparison_change, names='value')
    for widget in (comparison_select, left_input, right_input):
        widget.observe(run_comparison, names='value')

    # Call run_comparison immediately to compute initial output
    playground = widgets.VBox([comparison_select, left_input, right_input, output])

    run_comparison(None)

    return playground

playground = create_comparison_playground("column")
display(playground)

# %%
import splink.comparison_level_library as cll

first_name_comparison = cl.CustomComparison(
    comparison_levels=[
        cll.NullLevel("first_name"),
        cll.ExactMatchLevel("first_name"),
        {
            "sql_condition": "first_name_l = surname_r",
            "label_for_charts": "Match on reversed cols: first_name and surname",
        },
        cll.JaroWinklerLevel("first_name", 0.8),
        cll.ElseLevel(),
    ]
)
# Need to be able to pass values in as a dict {"first_name_l": "Robin", "first_name_r": "Robyn", "surname_l": "Linacre", "surname_r": "Linacre"}
values = {
    "first_name_l": "Robin",
    "first_name_r": "Linacre",
    "surname_l": "Linacre",
    "surname_r": "Robin",
}
display(Markdown(report_comparison_levels(first_name_comparison, values, "first_name")))
