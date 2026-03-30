from __future__ import annotations

import json
import math
import os
from abc import ABC, abstractmethod
from collections.abc import Iterable
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Literal,
    Protocol,
    Sequence,
    TypeVar,
    Union,
)

from splink.internals.misc import read_resource

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


# TODO: we can have more detailed subclasses to hint the fields needed per chart
class ChartRecord(Protocol): ...


T = TypeVar("T", bound=ChartRecord)


class SplinkChart(ABC, Generic[T]):
    def __init__(self, records: Sequence[T]):
        self.raw_records = records
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
    def altair_chart(self):
        from altair import Chart

        return Chart.from_dict(self.chart_dict)

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
        # TODO: do we need to be careful with charts that can't have these set
        # such as threshold_selection_tool
        self.width = width
        self.height = height
        # TODO: return self?

    def save(self, *args, **kwargs):
        self.altair_chart.save(*args, **kwargs)

    def save_offline_chart(
        self,
        filename: str = "my_chart.html",
        overwrite: bool = False,
        print_msg: bool = True,
    ) -> None:
        """
        Save Splink charting outputs to disk as a standalone .html file
        which works offline

        Args:
            filename (str, optional): Name of output file. Defaults to "my_chart.html".
            overwrite (bool, optional): Overwrite file if it exists. Defaults to False.
            print_msg (bool, optional): Print a message instructing the user how to view
                the outputs. Defaults to True.
        """

        iframe_message = """
        To view in Jupyter you can use the following command:

        from IPython.display import IFrame
        IFrame(src="./{filename}", width=1000, height=500)
        """

        if os.path.isfile(filename) and not overwrite:
            raise ValueError(
                f"The path {filename} already exists. Please provide a different path, "
                f"or set overwrite=True to overwrite."
            )

        template = read_resource("internals/files/templates/single_chart_template.html")

        fmt_dict = _load_external_libs()

        fmt_dict["mychart"] = json.dumps(self.chart_dict)

        with open(filename, "w", encoding="utf-8") as f:
            f.write(template.format(**fmt_dict))

        if print_msg:
            print(f"Chart saved to {filename}")  # noqa: T201
            print(iframe_message.format(filename=filename))  # noqa: T201

    # allows rich representation of altair chart in IPython environments: https://ipython.readthedocs.io/en/stable/config/integrating.html
    def _repr_mimebundle_(self, *args, **kwargs):
        # let altair handle the display for us
        return self.altair_chart._repr_mimebundle_(*args, **kwargs)


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
    ):
        super().__init__(records)
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


class WaterfallChart(SplinkChart[ChartRecord]):
    def __init__(
        self,
        records: Sequence[ChartRecord],
        filter_nulls: bool = True,
    ):
        super().__init__(records)
        self.filter_nulls = filter_nulls

    @property
    def chart_spec_file(self) -> str:
        return "match_weights_waterfall.json"

    def alter_spec_from_data(self, chart_spec):
        records = self.chart_data
        chart_spec["params"][0]["bind"]["max"] = len(records) - 1
        if self.filter_nulls:
            chart_spec["transform"].insert(
                1, {"filter": "(datum.bayes_factor !== 1.0)"}
            )
        return chart_spec


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


class AccuracyChart(SplinkChart[ChartRecord]):
    def __init__(
        self,
        records: Sequence[ChartRecord],
        add_metrics: Sequence[str] = [],  # TODO
    ):
        super().__init__(records)
        # User-specified metrics to include
        self.additional_metrics = add_metrics

    @property
    def chart_spec_file(self) -> str:
        return "accuracy_chart.json"

    @property
    def default_width(self) -> float:
        return 400

    @property
    def default_height(self) -> float:
        return 400

    @property
    def metrics(self):
        return ["precision", "recall", *self.additional_metrics]

    @staticmethod
    def alter_spec_directly(chart_spec):
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
        chart_spec["transform"][2]["calculate"] = chart_spec["transform"][2][
            "calculate"
        ].replace("__mapping__", str(mapping))
        chart_spec["layer"][0]["encoding"]["color"]["legend"]["labelExpr"] = chart_spec[
            "layer"
        ][0]["encoding"]["color"]["legend"]["labelExpr"].replace(
            "__mapping__", str(mapping)
        )

        return chart_spec

    def alter_spec_from_data(self, chart_spec):
        metrics = self.metrics
        chart_spec["transform"][0]["fold"] = metrics
        chart_spec["transform"][1]["calculate"] = chart_spec["transform"][1][
            "calculate"
        ].replace("__metrics__", str(metrics))
        chart_spec["layer"][0]["encoding"]["color"]["sort"] = metrics
        chart_spec["layer"][1]["layer"][1]["encoding"]["color"]["sort"] = metrics
        return chart_spec


class ThresholdSelectionToolChart(SplinkChart[ChartRecord]):
    def __init__(
        self,
        records: Sequence[ChartRecord],
        add_metrics: Sequence[str] = [],  # TODO
    ):
        super().__init__(records)
        # User-specified metrics to include
        self.additional_metrics = add_metrics

    @property
    def chart_spec_file(self) -> str:
        return "threshold_selection_tool.json"

    @property
    def metrics(self):
        return ["precision", "recall", *self.additional_metrics]

    @staticmethod
    def alter_data(records):
        return [d for d in records if d["precision"] > 0.5 and d["recall"] > 0.5]

    @staticmethod
    def alter_spec_directly(chart_spec):
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
        chart_spec["hconcat"][1]["transform"][2]["calculate"] = chart_spec["hconcat"][
            1
        ]["transform"][2]["calculate"].replace("__mapping__", str(mapping))
        chart_spec["hconcat"][1]["layer"][0]["encoding"]["color"]["legend"][
            "labelExpr"
        ] = chart_spec["hconcat"][1]["layer"][0]["encoding"]["color"]["legend"][
            "labelExpr"
        ].replace("__mapping__", str(mapping))

        return chart_spec

    def alter_spec_from_data(self, chart_spec):
        metrics = self.metrics
        chart_spec["hconcat"][1]["transform"][0]["fold"] = metrics
        chart_spec["hconcat"][1]["transform"][1]["calculate"] = chart_spec["hconcat"][
            1
        ]["transform"][1]["calculate"].replace("__metrics__", str(metrics))
        chart_spec["hconcat"][1]["layer"][0]["encoding"]["color"]["sort"] = metrics
        chart_spec["hconcat"][1]["layer"][1]["layer"][1]["encoding"]["color"][
            "sort"
        ] = metrics

        return chart_spec


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


class UnlinkablesChart(SplinkChart[ChartRecord]):
    def __init__(
        self,
        records: Sequence[ChartRecord],
        x_col: Literal["match_weight", "match_probability"] = "match_weight",
        source_dataset: str | None = None,
    ):
        if x_col not in ["match_weight", "match_probability"]:
            raise ValueError(
                f"{x_col} must be 'match_weight' (default) or 'match_probability'."
            )
        super().__init__(records)
        self.x_col = x_col
        self.source_dataset = source_dataset

    @property
    def chart_spec_file(self) -> str:
        return "unlinkables_chart_def.json"

    def alter_spec_from_data(self, chart_spec):
        if source_dataset := self.source_dataset:
            chart_spec["title"]["text"] += f" - {source_dataset}"
        if self.x_col == "match_weight":
            return chart_spec
        # if we have match_probability we need to update spec to match:
        chart_spec["layer"][0]["encoding"]["x"]["field"] = "match_probability"
        chart_spec["layer"][0]["encoding"]["x"]["axis"]["title"] = (
            "Threshold match probability"
        )
        chart_spec["layer"][0]["encoding"]["x"]["axis"]["format"] = ".2"

        chart_spec["layer"][1]["encoding"]["x"]["field"] = "match_probability"
        chart_spec["layer"][1]["selection"]["selector112"]["fields"] = [
            "match_probability",
            "cum_prop",
        ]

        chart_spec["layer"][2]["encoding"]["x"]["field"] = "match_probability"
        chart_spec["layer"][2]["encoding"]["x"]["axis"]["title"] = (
            "Threshold match probability"
        )

        chart_spec["layer"][3]["encoding"]["x"]["field"] = "match_probability"

        return chart_spec


class CompletenessChart(SplinkChart[ChartRecord]):
    @property
    def chart_spec_file(self) -> str:
        return "completeness.json"


class CumulativeBlockingRuleComparisonsGeneratedChart(SplinkChart[ChartRecord]):
    @property
    def chart_spec_file(self) -> str:
        return "blocking_rule_generated_comparisons.json"


class TFAdjustmentChart(SplinkChart[ChartRecord]):
    def __init__(
        self,
        records: Sequence[ChartRecord],
        hist_records: Sequence[ChartRecord],
        tf_comparison_records: Sequence[ComparisonLevelDetailedRecord],
    ):
        super().__init__(records)
        self.hist_records = hist_records
        self.tf_levels = [cl.comparison_vector_value for cl in tf_comparison_records]
        self.labels = [
            f"{cl.label_for_charts} (TF col: {cl.tf_adjustment_column})"
            for cl in tf_comparison_records
        ]

    @property
    def chart_spec_file(self) -> str:
        return "tf_adjustment_chart.json"

    @property
    def chart_dict(self) -> dict[str, Any]:
        chart = self.chart_spec
        chart["datasets"]["data"] = self.raw_records
        chart["datasets"]["hist"] = self.hist_records
        return chart

    @staticmethod
    def alter_spec_directly(chart_spec):
        # filters = [
        #     f"datum.most_freq_rank < {n_most_freq}",
        #     f"datum.least_freq_rank < {n_least_freq}",
        #     " | ".join([f"datum.value == '{v}'" for v in vals_to_include])
        # ]
        # filter_text = " | ".join(filters)
        # chart["hconcat"][0]["layer"][0]["transform"][2]["filter"] = filter_text
        # chart["hconcat"][0]["layer"][2]["transform"][2]["filter"] = filter_text

        # PLACEHOLDER (until we work out adding a dynamic title based on the
        # filtered data)
        chart_spec["hconcat"][0]["layer"][0]["encoding"]["x"]["title"] = (
            "TF column value"
        )
        chart_spec["hconcat"][0]["layer"][-1]["encoding"]["x"]["title"] = (
            "TF column value"
        )
        chart_spec["hconcat"][0]["layer"][0]["encoding"]["tooltip"][0]["title"] = (
            "Value"
        )
        return chart_spec

    def alter_spec_from_data(self, chart_spec):
        chart_spec["config"]["params"][0]["value"] = max(self.tf_levels)
        chart_spec["config"]["params"][0]["bind"]["options"] = self.tf_levels
        chart_spec["config"]["params"][0]["bind"]["labels"] = self.labels
        return chart_spec


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
