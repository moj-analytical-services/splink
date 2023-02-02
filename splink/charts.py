import json
import os
import pkgutil

from .waterfall_chart import records_to_waterfall_data

altair_installed = True
try:

    from altair.vegalite.v4.display import VegaLite

    # Slightly re-write logic to avoid validation
    # Some splink3 charts do not validate but display fine
    # When Altair supports Vega Lite v5, this should no longer be a problem
    # and this logic should be able to be removed
    class VegaliteNoValidate(VegaLite):
        def _validate(self):
            pass

    def vegalite_no_validate(spec):

        return VegaliteNoValidate(spec)

except ImportError:
    altair_installed = False


def load_chart_definition(filename):
    path = f"files/chart_defs/{filename}"
    data = pkgutil.get_data(__name__, path)
    schema = json.loads(data)
    return schema


def _load_external_libs():

    to_load = {
        "vega-embed": "files/external_js/vega-embed@6.20.2",
        "vega-lite": "files/external_js/vega-lite@5.2.0",
        "vega": "files/external_js/vega@5.21.0",
    }

    loaded = {}
    for k, v in to_load.items():
        script = pkgutil.get_data(__name__, v).decode("utf-8")
        loaded[k] = script

    return loaded


def vegalite_or_json(chart_dict, as_dict=False):

    if altair_installed:
        if not as_dict:
            try:
                # Display chart then return its spec
                return vegalite_no_validate(chart_dict)

            except ModuleNotFoundError:
                return chart_dict

    return chart_dict


iframe_message = """
To view in Jupyter you can use the following command:

from IPython.display import IFrame
IFrame(src="./{filename}", width=1000, height=500)
"""


def save_offline_chart(
    chart_dict, filename="my_chart.html", overwrite=False, print_msg=True
):
    """Save Splink charting outputs to disk as a standalone .html file
    which works offline

    Args:
        chart_dict (dict): A vega lite spec as a dictionary
        filename (str, optional): Name of output file. Defaults to "my_chart.html".
        overwrite (bool, optional): Overwrite file if it exists. Defaults to False.
        print_msg (bool, optional): Print a message instructing the user how to view
            the outputs. Defaults to True.

    """

    if os.path.isfile(filename) and not overwrite:
        raise ValueError(
            f"The path {filename} already exists. Please provide a different path."
        )

    if type(chart_dict).__name__ == "VegaliteNoValidate":
        chart_dict = chart_dict.spec

    # get altair chart as json
    path = "files/templates/single_chart_template.txt"
    template = pkgutil.get_data(__name__, path).decode("utf-8")

    fmt_dict = _load_external_libs()

    fmt_dict["mychart"] = json.dumps(chart_dict)

    with open(filename, "w", encoding="utf-8") as f:
        f.write(template.format(**fmt_dict))

    if print_msg:
        print(f"Chart saved to {filename}")
        print(iframe_message.format(filename=filename))


def match_weights_chart(records, as_dict=False):
    chart_path = "match_weights_interactive_history.json"
    chart = load_chart_definition(chart_path)

    # Remove iteration history since this is a static chart
    del chart["params"]
    del chart["transform"]

    records = [r for r in records if r["comparison_vector_value"] != -1]
    chart["data"]["values"] = records
    return vegalite_or_json(chart, as_dict=as_dict)


def comparison_match_weights_chart(records, as_dict=False):
    chart_path = "match_weights_interactive_history.json"
    chart = load_chart_definition(chart_path)

    # Remove iteration history since this is a static chart
    del chart["vconcat"][0]
    del chart["params"]
    del chart["transform"]

    chart["title"]["text"] = "Comparison summary"
    records = [r for r in records if r["comparison_vector_value"] != -1]
    chart["data"]["values"] = records
    return vegalite_or_json(chart, as_dict=as_dict)


def m_u_parameters_chart(records, as_dict=False):
    chart_path = "m_u_parameters_interactive_history.json"
    chart = load_chart_definition(chart_path)

    # Remove iteration history since this is a static chart
    del chart["params"]
    del chart["transform"]

    records = [
        r
        for r in records
        if r["comparison_vector_value"] != -1
        and r["comparison_name"] != "probability_two_random_records_match"
    ]
    chart["data"]["values"] = records
    return vegalite_or_json(chart, as_dict=as_dict)


def probability_two_random_records_match_iteration_chart(records, as_dict=False):
    chart_path = "probability_two_random_records_match_iteration.json"
    chart = load_chart_definition(chart_path)

    chart["data"]["values"] = records
    return vegalite_or_json(chart, as_dict=as_dict)


def match_weights_interactive_history_chart(records, as_dict=False, blocking_rule=None):
    chart_path = "match_weights_interactive_history.json"
    chart = load_chart_definition(chart_path)

    chart["title"]["subtitle"] = f"Training session blocked on {blocking_rule}"

    records = [r for r in records if r["comparison_vector_value"] != -1]
    chart["data"]["values"] = records

    max_iteration = 0
    for r in records:
        max_iteration = max(r["iteration"], max_iteration)

    chart["params"][0]["bind"]["max"] = max_iteration
    chart["params"][0]["value"] = max_iteration
    return vegalite_or_json(chart, as_dict=as_dict)


def m_u_parameters_interactive_history_chart(records, as_dict=False):
    chart_path = "m_u_parameters_interactive_history.json"
    chart = load_chart_definition(chart_path)
    records = [
        r
        for r in records
        if r["comparison_vector_value"] != -1
        and r["comparison_name"] != "probability_two_random_records_match"
    ]
    chart["data"]["values"] = records

    max_iteration = 0
    for r in records:
        max_iteration = max(r["iteration"], max_iteration)

    chart["params"][0]["bind"]["max"] = max_iteration
    chart["params"][0]["value"] = max_iteration
    return vegalite_or_json(chart, as_dict=as_dict)


def waterfall_chart(
    records,
    settings_obj,
    filter_nulls=True,
    as_dict=False,
):
    data = records_to_waterfall_data(records, settings_obj)
    chart_path = "match_weights_waterfall.json"
    chart = load_chart_definition(chart_path)
    chart["data"]["values"] = data
    chart["params"][0]["bind"]["max"] = len(records) - 1
    if filter_nulls:
        chart["transform"].insert(1, {"filter": "(datum.bayes_factor !== 1.0)"})

    return vegalite_or_json(chart, as_dict=as_dict)


def roc_chart(records, width=400, height=400, as_dict=False):
    chart_path = "roc.json"
    chart = load_chart_definition(chart_path)

    chart["data"]["values"] = records

    # If 'curve_label' not in records, remove colour coding
    # This is for if you want to compare roc curves
    r = records[0]
    if "curve_label" not in r.keys():
        del chart["encoding"]["color"]

    chart["height"] = height
    chart["width"] = width

    return vegalite_or_json(chart, as_dict=as_dict)


def precision_recall_chart(records, width=400, height=400, as_dict=False):
    chart_path = "precision_recall.json"
    chart = load_chart_definition(chart_path)

    chart["data"]["values"] = records

    # If 'curve_label' not in records, remove colour coding
    # This is for if you want to compare roc curves
    r = records[0]
    if "curve_label" not in r.keys():
        del chart["encoding"]["color"]

    chart["height"] = height
    chart["width"] = width

    return vegalite_or_json(chart, as_dict=as_dict)


def match_weights_histogram(records, width=500, height=250, as_dict=False):
    chart_path = "match_weight_histogram.json"
    chart = load_chart_definition(chart_path)

    chart["data"]["values"] = records

    chart["height"] = height
    chart["width"] = width

    return vegalite_or_json(chart, as_dict=as_dict)


def parameter_estimate_comparisons(records, as_dict=False):
    chart_path = "parameter_estimate_comparisons.json"
    chart = load_chart_definition(chart_path)

    chart["data"]["values"] = records

    return vegalite_or_json(chart, as_dict=as_dict)


def missingness_chart(records, as_dict=False):
    chart_path = "missingness.json"
    chart = load_chart_definition(chart_path)

    chart["data"]["values"] = records

    record_count = records[0]["total_record_count"]

    for c in chart["layer"]:
        c["title"] = f"Missingness per column out of {record_count:,.0f} records"

    return vegalite_or_json(chart, as_dict=as_dict)


def unlinkables_chart(
    records,
    x_col="match_weight",
    source_dataset=None,
    as_dict=False,
):

    if x_col not in ["match_weight", "match_probability"]:
        raise ValueError(
            f"{x_col} must be 'match_weight' (default) or 'match_probability'."
        )

    chart_path = "unlinkables_chart_def.json"
    unlinkables_chart_def = load_chart_definition(chart_path)
    unlinkables_chart_def["data"]["values"] = records

    if x_col == "match_probability":
        unlinkables_chart_def["layer"][0]["encoding"]["x"][
            "field"
        ] = "match_probability"
        unlinkables_chart_def["layer"][0]["encoding"]["x"]["axis"][
            "title"
        ] = "Threshold match probability"
        unlinkables_chart_def["layer"][0]["encoding"]["x"]["axis"]["format"] = ".2"

        unlinkables_chart_def["layer"][1]["encoding"]["x"][
            "field"
        ] = "match_probability"
        unlinkables_chart_def["layer"][1]["selection"]["selector112"]["fields"] = [
            "match_probability",
            "cum_prop",
        ]

        unlinkables_chart_def["layer"][2]["encoding"]["x"][
            "field"
        ] = "match_probability"
        unlinkables_chart_def["layer"][2]["encoding"]["x"]["axis"][
            "title"
        ] = "Threshold match probability"

        unlinkables_chart_def["layer"][3]["encoding"]["x"][
            "field"
        ] = "match_probability"

    if source_dataset:
        unlinkables_chart_def["title"]["text"] += f" - {source_dataset}"

    return vegalite_or_json(unlinkables_chart_def, as_dict=as_dict)


def completeness_chart(records, as_dict=False):
    chart_path = "completeness.json"
    chart = load_chart_definition(chart_path)

    chart["data"]["values"] = records

    return vegalite_or_json(chart, as_dict=as_dict)


def cumulative_blocking_rule_comparisons_generated(records, as_dict=False):
    chart_path = "blocking_rule_generated_comparisons.json"
    chart = load_chart_definition(chart_path)

    chart["data"]["values"] = records

    return vegalite_or_json(chart, as_dict=as_dict)
