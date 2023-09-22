import json
import math
import os

import numpy as np
import pandas as pd

from .misc import read_resource
from .waterfall_chart import records_to_waterfall_data

altair_installed = True

try:
    import altair as alt
except ImportError:
    altair_installed = False


def load_chart_definition(filename):
    path = f"files/chart_defs/{filename}"
    return json.loads(read_resource(path))


def _load_external_libs():
    to_load = {
        "vega-embed": "files/external_js/vega-embed@6.20.2",
        "vega-lite": "files/external_js/vega-lite@5.2.0",
        "vega": "files/external_js/vega@5.21.0",
    }
    return {k: read_resource(v) for k, v in to_load.items()}


def altair_or_json(chart_dict, as_dict=False):
    if altair_installed:
        if not as_dict:
            try:
                return alt.Chart.from_dict(chart_dict)

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

    template = read_resource("files/templates/single_chart_template.html")

    fmt_dict = _load_external_libs()

    fmt_dict["mychart"] = json.dumps(chart_dict)

    with open(filename, "w", encoding="utf-8") as f:
        f.write(template.format(**fmt_dict))

    if print_msg:
        print(f"Chart saved to {filename}")  # noqa: T201
        print(iframe_message.format(filename=filename))  # noqa: T201


def match_weights_chart(records, as_dict=False):
    chart_path = "match_weights_interactive_history.json"
    chart = load_chart_definition(chart_path)

    # Remove iteration history since this is a static chart
    del chart["params"]
    del chart["transform"]

    records = [r for r in records if r["comparison_vector_value"] != -1]
    chart["data"]["values"] = records

    df = pd.DataFrame.from_records(records)["log2_bayes_factor"]
    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.dropna()

    max_value = df.abs().max()
    max_value = math.ceil(max_value)

    chart["vconcat"][0]["encoding"]["x"]["scale"]["domain"] = [-max_value, max_value]
    chart["vconcat"][1]["encoding"]["x"]["scale"]["domain"] = [-max_value, max_value]

    return altair_or_json(chart, as_dict=as_dict)


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
    return altair_or_json(chart, as_dict=as_dict)


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
    return altair_or_json(chart, as_dict=as_dict)


def probability_two_random_records_match_iteration_chart(records, as_dict=False):
    chart_path = "probability_two_random_records_match_iteration.json"
    chart = load_chart_definition(chart_path)

    chart["data"]["values"] = records
    return altair_or_json(chart, as_dict=as_dict)


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
    return altair_or_json(chart, as_dict=as_dict)


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
    return altair_or_json(chart, as_dict=as_dict)


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

    return altair_or_json(chart, as_dict=as_dict)


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

    return altair_or_json(chart, as_dict=as_dict)


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

    return altair_or_json(chart, as_dict=as_dict)


def accuracy_chart(records, width=400, height=400, as_dict=False, add_metrics=[]):
    chart_path = "accuracy_chart.json"
    chart = load_chart_definition(chart_path)

    # User-specified metrics to include
    metrics = ["precision", "recall", *add_metrics]
    chart["transform"][0]["fold"] = metrics
    chart["transform"][1]["calculate"] = chart["transform"][1]["calculate"].replace(
        "__metrics__", str(metrics)
    )
    chart["layer"][0]["encoding"]["color"]["sort"] = metrics
    chart["layer"][1]["layer"][1]["encoding"]["color"]["sort"] = metrics

    # Metric-label mapping
    mapping = {
        "precision": "Precision (PPV)",
        "recall": "Recall (TPR)",
        "specificity": "Specificity (TNR)",
        "accuracy": "Accuracy",
        "npv": "NPV",
        "f1": "F1",
        "f2": "F2",
        "f0_5": "F0.5",
        "p4": "P4",
        "phi": "\u03C6 (MCC)",
    }
    chart["transform"][2]["calculate"] = chart["transform"][2]["calculate"].replace(
        "__mapping__", str(mapping)
    )
    chart["layer"][0]["encoding"]["color"]["legend"]["labelExpr"] = chart["layer"][0][
        "encoding"
    ]["color"]["legend"]["labelExpr"].replace("__mapping__", str(mapping))

    chart["data"]["values"] = records

    chart["height"] = height
    chart["width"] = width

    return altair_or_json(chart, as_dict=as_dict)


def confusion_matrix_chart(records, match_weight_range=[-15, 15], as_dict=False):
    chart_path = "confusion_matrix.json"
    chart = load_chart_definition(chart_path)

    chart["data"]["values"] = records

    chart["hconcat"][0]["encoding"]["x"]["scale"]["domain"] = match_weight_range

    return altair_or_json(chart, as_dict=as_dict)


def match_weights_histogram(records, width=500, height=250, as_dict=False):
    chart_path = "match_weight_histogram.json"
    chart = load_chart_definition(chart_path)

    chart["data"]["values"] = records

    chart["height"] = height
    chart["width"] = width

    return altair_or_json(chart, as_dict=as_dict)


def parameter_estimate_comparisons(records, as_dict=False):
    chart_path = "parameter_estimate_comparisons.json"
    chart = load_chart_definition(chart_path)

    chart["data"]["values"] = records

    return altair_or_json(chart, as_dict=as_dict)


def missingness_chart(records, as_dict=False):
    chart_path = "missingness.json"
    chart = load_chart_definition(chart_path)

    chart["data"]["values"] = records

    record_count = records[0]["total_record_count"]

    for c in chart["layer"]:
        c["title"] = f"Missingness per column out of {record_count:,.0f} records"

    return altair_or_json(chart, as_dict=as_dict)


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

    return altair_or_json(unlinkables_chart_def, as_dict=as_dict)


def completeness_chart(records, as_dict=False):
    chart_path = "completeness.json"
    chart = load_chart_definition(chart_path)

    chart["data"]["values"] = records

    return altair_or_json(chart, as_dict=as_dict)


def cumulative_blocking_rule_comparisons_generated(records, as_dict=False):
    chart_path = "blocking_rule_generated_comparisons.json"
    chart = load_chart_definition(chart_path)

    chart["data"]["values"] = records

    return altair_or_json(chart, as_dict=as_dict)


def _comparator_score_chart(similarity_records, distance_records, as_dict=False):
    chart_path = "comparator_score_chart.json"
    chart = load_chart_definition(chart_path)

    chart["datasets"]["data-similarity"] = similarity_records
    chart["datasets"]["data-distance"] = distance_records

    return altair_or_json(chart, as_dict=as_dict)


def _comparator_score_threshold_chart(
    similarity_records,
    distance_records,
    similarity_threshold,
    distance_threshold,
    as_dict=False,
):
    chart_path = "comparator_score_threshold_chart.json"
    chart = load_chart_definition(chart_path)

    chart["params"][0]["value"] = similarity_threshold
    chart["params"][1]["value"] = distance_threshold

    chart["hconcat"][0]["layer"][0]["title"]["subtitle"] = f">= {similarity_threshold}"
    chart["hconcat"][1]["layer"][0]["title"]["subtitle"] = f"<= {distance_threshold}"

    chart["datasets"]["data-similarity"] = similarity_records
    chart["datasets"]["data-distance"] = distance_records

    return altair_or_json(chart, as_dict=as_dict)


def _phonetic_match_chart(records, as_dict=False):
    chart_path = "phonetic_match_chart.json"
    chart = load_chart_definition(chart_path)

    chart["datasets"]["data-phonetic"] = records

    return altair_or_json(chart, as_dict=as_dict)
