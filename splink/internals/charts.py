from __future__ import annotations

import json
import math
import os
from abc import ABC, abstractmethod
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, Generic, Protocol, Sequence, TypeVar, Union

from splink.internals.misc import read_resource
from splink.internals.waterfall_chart import records_to_waterfall_data

if TYPE_CHECKING:
    from altair import SchemaBase

    from splink.internals.comparison_level import ComparisonLevelDetailedRecord
    from splink.internals.em_training_session import (
        ModelParameterIterationDetailedRecord,
    )
    from splink.internals.settings import ModelParameterDetailedRecord
else:
    SchemaBase = None

    ComparisonLevelDetailedRecord = None
    ModelParameterDetailedRecord = None
    ModelParameterIterationDetailedRecord = None
# type alias:
ChartReturnType = Union[dict[Any, Any], SchemaBase]


def load_chart_definition(filename):
    path = f"internals/files/chart_defs/{filename}"
    return json.loads(read_resource(path))


def _load_external_libs():
    to_load = {
        "vega-embed": "internals/files/external_js/vega-embed@6.20.2",
        "vega-lite": "internals/files/external_js/vega-lite@5.2.0",
        "vega": "internals/files/external_js/vega@5.31.0",
    }
    return {k: read_resource(v) for k, v in to_load.items()}


def altair_or_json(
    chart_dict: dict[Any, Any], as_dict: bool = False
) -> ChartReturnType:
    from altair import Chart

    if not as_dict:
        return Chart.from_dict(chart_dict)

    return chart_dict


class AsDictable(Protocol):
    def as_dict(self) -> dict[str, Any]: ...


def list_items_as_dicts(
    lst: Iterable[AsDictable | dict[str, Any]],
) -> list[dict[str, Any]]:
    return list(
        map(lambda item: item if isinstance(item, dict) else item.as_dict(), lst)
    )


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

    template = read_resource("internals/files/templates/single_chart_template.html")

    fmt_dict = _load_external_libs()

    fmt_dict["mychart"] = json.dumps(chart_dict)

    with open(filename, "w", encoding="utf-8") as f:
        f.write(template.format(**fmt_dict))

    if print_msg:
        print(f"Chart saved to {filename}")  # noqa: T201
        print(iframe_message.format(filename=filename))  # noqa: T201


# TODO: we can have more detailed subclasses to hint the fields needed per chart
class ChartRecord(Protocol): ...


T = TypeVar("T", bound=ChartRecord)


class SplinkChart(ABC, Generic[T]):
    def __init__(self, records: Sequence[T], as_dict: bool = False):
        # TODO: as_dict only in methods rather than on object
        self.raw_records = records
        self.as_dict = as_dict
        self.width: float | None = self.default_width
        self.height: float | None = self.default_height

    @property
    @abstractmethod
    def chart_spec_file(self) -> str:
        pass

    @property
    def chart_data(self):
        # TODO: cache
        return self.alter_data(self.raw_records)

    @property
    def default_width(self) -> float | None:
        return None

    @property
    def default_height(self) -> float | None:
        return None

    @property
    def chart_spec(self) -> dict[str, Any]:
        chart_spec = load_chart_definition(self.chart_spec_file)
        chart_spec = self.alter_spec_directly(chart_spec)
        chart_spec = self.alter_spec_height_width(chart_spec)
        chart_spec = self.alter_spec_from_data(chart_spec)
        return chart_spec

    @property
    def chart_dict(self) -> dict[str, Any]:
        chart = self.chart_spec
        chart["data"]["values"] = list_items_as_dicts(self.chart_data)
        return chart

    @property
    def chart(self) -> ChartReturnType:
        # TODO: split into separate methods
        # also save etc
        return altair_or_json(self.chart_dict, as_dict=self.as_dict)

    @staticmethod
    def alter_data(records):
        return records

    @staticmethod
    def alter_spec_directly(chart_spec):
        return chart_spec

    def alter_spec_from_data(self, chart_spec):
        return chart_spec

    def alter_spec_height_width(self, chart_spec):
        if width := self.width:
            chart_spec["width"] = width
        if height := self.height:
            chart_spec["height"] = height
        return chart_spec

    def set_width_height(
        self, *, width: float | None = None, height: float | None = None
    ) -> None:
        self.width = width
        self.height = height
        # TODO: return self?


class MatchWeightsChart(SplinkChart[ComparisonLevelDetailedRecord]):
    @property
    def chart_spec_file(self) -> str:
        return "match_weights_interactive_history.json"

    @staticmethod
    def alter_data(records):
        return [r for r in records if r.comparison_vector_value != -1]

    @staticmethod
    def alter_spec_directly(chart_spec):
        # Remove iteration history since this is a static chart
        del chart_spec["params"]
        del chart_spec["transform"]
        return chart_spec

    def alter_spec_from_data(self, chart_spec):
        bayes_factors = [
            abs(l2bf)
            for r in self.chart_data
            if (l2bf := r.log2_bayes_factor) is not None and not math.isinf(l2bf)
        ]
        max_value = math.ceil(max(bayes_factors))
        chart_spec["vconcat"][0]["encoding"]["x"]["scale"]["domain"] = [
            -max_value,
            max_value,
        ]
        chart_spec["vconcat"][1]["encoding"]["x"]["scale"]["domain"] = [
            -max_value,
            max_value,
        ]
        return chart_spec


class ComparisonMatchWeightsChart(SplinkChart[ComparisonLevelDetailedRecord]):
    @property
    def chart_spec_file(self) -> str:
        return "match_weights_interactive_history.json"

    @staticmethod
    def alter_data(records):
        return [r for r in records if r.comparison_vector_value != -1]

    @staticmethod
    def alter_spec_directly(chart_spec):
        # Remove iteration history since this is a static chart
        # TODO: some render issue if we remove empty top panel, so leave for now
        # del chart["vconcat"][0]
        del chart_spec["params"]
        del chart_spec["transform"]
        chart_spec["title"]["text"] = "Comparison summary"
        return chart_spec


class MUParametersChart(SplinkChart[ModelParameterDetailedRecord]):
    @property
    def chart_spec_file(self) -> str:
        return "m_u_parameters_interactive_history.json"

    @staticmethod
    def alter_data(records):
        return [
            r
            for r in records
            if r.comparison_vector_value != -1
            and r.comparison_name != "probability_two_random_records_match"
        ]

    @staticmethod
    def alter_spec_directly(chart_spec):
        # Remove iteration history since this is a static chart
        del chart_spec["params"]
        del chart_spec["transform"]
        return chart_spec


class ProbabilityTwoRandomRecordsMatchIterationChart(SplinkChart[ChartRecord]):
    @property
    def chart_spec_file(self) -> str:
        return "probability_two_random_records_match_iteration.json"


class MatchWeightsInteractiveHistoryChart(
    SplinkChart[ModelParameterIterationDetailedRecord]
):
    def __init__(
        self,
        records: Sequence[ModelParameterIterationDetailedRecord],
        blocking_rule_text: str,
        as_dict: bool = False,
    ):
        super().__init__(records, as_dict=as_dict)
        self.blocking_rule_text = blocking_rule_text

    @property
    def chart_spec_file(self) -> str:
        return "match_weights_interactive_history.json"

    @staticmethod
    def alter_data(records):
        return [r for r in records if r.comparison_vector_value != -1]

    def alter_spec_from_data(self, chart_spec):
        records = self.chart_data
        max_iteration = 0
        for r in records:
            max_iteration = max(r.iteration, max_iteration)

        chart_spec["params"][0]["bind"]["max"] = max_iteration
        chart_spec["params"][0]["value"] = max_iteration
        chart_spec["title"]["subtitle"] = (
            f"Training session blocked on {self.blocking_rule_text}"
        )
        return chart_spec


class MUParametersInteractiveHistoryChart(
    SplinkChart[ModelParameterIterationDetailedRecord]
):
    @property
    def chart_spec_file(self) -> str:
        return "m_u_parameters_interactive_history.json"

    @staticmethod
    def alter_data(records):
        return [
            r
            for r in records
            if r.comparison_vector_value != -1
            and r.comparison_name != "probability_two_random_records_match"
        ]

    def alter_spec_from_data(self, chart_spec):
        records = self.chart_data
        max_iteration = 0
        for r in records:
            max_iteration = max(r.iteration, max_iteration)

        chart_spec["params"][0]["bind"]["max"] = max_iteration
        chart_spec["params"][0]["value"] = max_iteration
        return chart_spec


def waterfall_chart(
    records,
    settings_obj,
    filter_nulls=True,
    remove_sensitive_data=False,
    as_dict=False,
):
    data = records_to_waterfall_data(records, settings_obj, remove_sensitive_data)
    chart_path = "match_weights_waterfall.json"
    chart = load_chart_definition(chart_path)
    chart["data"]["values"] = data
    chart["params"][0]["bind"]["max"] = len(records) - 1
    if filter_nulls:
        chart["transform"].insert(1, {"filter": "(datum.bayes_factor !== 1.0)"})

    return altair_or_json(chart, as_dict=as_dict)


class ROCChart(SplinkChart[ChartRecord]):
    @property
    def chart_spec_file(self) -> str:
        return "roc.json"

    @property
    def default_width(self) -> float:
        return 400

    @property
    def default_height(self) -> float:
        return 400

    def alter_spec_from_data(self, chart_spec):
        records = self.chart_data
        # If 'curve_label' not in records, remove colour coding
        # This is for if you want to compare roc curves
        r = records[0]
        if "curve_label" not in r.keys():
            del chart_spec["encoding"]["color"]
        return chart_spec


class PrecisionRecallChart(SplinkChart[ChartRecord]):
    @property
    def chart_spec_file(self) -> str:
        return "precision_recall.json"

    @property
    def default_width(self) -> float:
        return 400

    @property
    def default_height(self) -> float:
        return 400

    def alter_spec_from_data(self, chart_spec):
        records = self.chart_data
        # If 'curve_label' not in records, remove colour coding
        # This is for if you want to compare roc curves
        r = records[0]
        if "curve_label" not in r.keys():
            del chart_spec["encoding"]["color"]
        return chart_spec


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
        "phi": "\u03c6 (MCC)",
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


def threshold_selection_tool(records, as_dict=False, add_metrics=[]):
    chart_path = "threshold_selection_tool.json"
    chart = load_chart_definition(chart_path)

    # Remove extremes with low precision and recall
    records = [d for d in records if d["precision"] > 0.5 and d["recall"] > 0.5]

    # User-specified metrics to include
    metrics = ["precision", "recall", *add_metrics]

    chart["hconcat"][1]["transform"][0]["fold"] = metrics
    chart["hconcat"][1]["transform"][1]["calculate"] = chart["hconcat"][1]["transform"][
        1
    ]["calculate"].replace("__metrics__", str(metrics))
    chart["hconcat"][1]["layer"][0]["encoding"]["color"]["sort"] = metrics
    chart["hconcat"][1]["layer"][1]["layer"][1]["encoding"]["color"]["sort"] = metrics

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
        "phi": "\u03c6 (MCC)",
    }
    chart["hconcat"][1]["transform"][2]["calculate"] = chart["hconcat"][1]["transform"][
        2
    ]["calculate"].replace("__mapping__", str(mapping))
    chart["hconcat"][1]["layer"][0]["encoding"]["color"]["legend"]["labelExpr"] = chart[
        "hconcat"
    ][1]["layer"][0]["encoding"]["color"]["legend"]["labelExpr"].replace(
        "__mapping__", str(mapping)
    )

    chart["data"]["values"] = records

    return altair_or_json(chart, as_dict=as_dict)


class MatchWeightsHistogramChart(SplinkChart[ChartRecord]):
    @property
    def chart_spec_file(self) -> str:
        return "match_weight_histogram.json"

    @property
    def default_width(self) -> float:
        return 500

    @property
    def default_height(self) -> float:
        return 250


class ParameterEstimateComparisonsChart(SplinkChart[ChartRecord]):
    @property
    def chart_spec_file(self) -> str:
        return "parameter_estimate_comparisons.json"


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
        unlinkables_chart_def["layer"][0]["encoding"]["x"]["field"] = (
            "match_probability"
        )
        unlinkables_chart_def["layer"][0]["encoding"]["x"]["axis"]["title"] = (
            "Threshold match probability"
        )
        unlinkables_chart_def["layer"][0]["encoding"]["x"]["axis"]["format"] = ".2"

        unlinkables_chart_def["layer"][1]["encoding"]["x"]["field"] = (
            "match_probability"
        )
        unlinkables_chart_def["layer"][1]["selection"]["selector112"]["fields"] = [
            "match_probability",
            "cum_prop",
        ]

        unlinkables_chart_def["layer"][2]["encoding"]["x"]["field"] = (
            "match_probability"
        )
        unlinkables_chart_def["layer"][2]["encoding"]["x"]["axis"]["title"] = (
            "Threshold match probability"
        )

        unlinkables_chart_def["layer"][3]["encoding"]["x"]["field"] = (
            "match_probability"
        )

    if source_dataset:
        unlinkables_chart_def["title"]["text"] += f" - {source_dataset}"

    return altair_or_json(unlinkables_chart_def, as_dict=as_dict)


class CompletenessChart(SplinkChart[ChartRecord]):
    @property
    def chart_spec_file(self) -> str:
        return "completeness.json"


class CumulativeBlockingRuleComparisonsGeneratedChart(SplinkChart[ChartRecord]):
    @property
    def chart_spec_file(self) -> str:
        return "blocking_rule_generated_comparisons.json"


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
