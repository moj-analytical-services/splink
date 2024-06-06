from __future__ import annotations

from typing import List, Literal, Union

from splink.internals.accuracy import (
    prediction_errors_from_label_column,
    prediction_errors_from_labels_table,
    truth_space_table_from_labels_column,
    truth_space_table_from_labels_table,
)
from splink.internals.charts import (
    ChartReturnType,
    accuracy_chart,
    precision_recall_chart,
    roc_chart,
    threshold_selection_tool,
)
from splink.internals.splink_dataframe import SplinkDataFrame


class LinkerEvalution:
    def __init__(self, linker):
        self._linker = linker

    def prediction_errors_from_labels_table(
        self,
        labels_splinkdataframe_or_table_name,
        include_false_positives=True,
        include_false_negatives=True,
        threshold=0.5,
    ):
        """Generate a dataframe containing false positives and false negatives
        based on the comparison between the clerical_match_score in the labels
        table compared with the splink predicted match probability

        Args:
            labels_splinkdataframe_or_table_name (str | SplinkDataFrame): Name of table
                containing labels in the database
            include_false_positives (bool, optional): Defaults to True.
            include_false_negatives (bool, optional): Defaults to True.
            threshold (float, optional): Threshold above which a score is considered
                to be a match. Defaults to 0.5.

        Returns:
            SplinkDataFrame:  Table containing false positives and negatives
        """
        labels_tablename = self._get_labels_tablename_from_input(
            labels_splinkdataframe_or_table_name
        )
        return prediction_errors_from_labels_table(
            self,
            labels_tablename,
            include_false_positives,
            include_false_negatives,
            threshold,
        )

    def accuracy_analysis_from_labels_column(
        self,
        labels_column_name: str,
        *,
        threshold_actual: float = 0.5,
        match_weight_round_to_nearest: float = 0.1,
        output_type: Literal[
            "threshold_selection", "roc", "precision_recall", "table", "accuracy"
        ] = "threshold_selection",
        add_metrics: List[
            Literal[
                "specificity",
                "npv",
                "accuracy",
                "f1",
                "f2",
                "f0_5",
                "p4",
                "phi",
            ]
        ] = [],
        positives_not_captured_by_blocking_rules_scored_as_zero: bool = True,
    ) -> Union[ChartReturnType, SplinkDataFrame]:
        """Generate an accuracy chart or table from ground truth data, where the ground
        truth is in a column in the input dataset called `labels_column_name`

        Args:
            labels_column_name (str): Column name containing labels in the input table
            threshold_actual (float, optional): Where the `clerical_match_score`
                provided by the user is a probability rather than binary, this value
                is used as the threshold to classify `clerical_match_score`s as binary
                matches or non matches. Defaults to 0.5.
            match_weight_round_to_nearest (float, optional): When provided, thresholds
                are rounded.  When large numbers of labels are provided, this is
                sometimes necessary to reduce the size of the ROC table, and therefore
                the number of points plotted on the chart. Defaults to None.
            add_metrics (list(str), optional): Precision and recall metrics are always
                included. Where provided, `add_metrics` specifies additional metrics
                to show, with the following options:

                - `"specificity"`: specificity, selectivity, true negative rate (TNR)
                - `"npv"`: negative predictive value (NPV)
                - `"accuracy"`: overall accuracy (TP+TN)/(P+N)
                - `"f1"`/`"f2"`/`"f0_5"`: F-scores for \u03b2=1 (balanced), \u03b2=2
                (emphasis on recall) and \u03b2=0.5 (emphasis on precision)
                - `"p4"` -  an extended F1 score with specificity and NPV included
                - `"phi"` - \u03c6 coefficient or Matthews correlation coefficient (MCC)
        Examples:
            ```py
            linker.accuracy_analysis_from_labels_column("ground_truth", add_metrics=["f1"])
            ```

        Returns:
            altair.Chart: An altair chart
        """  # noqa: E501

        allowed = ["specificity", "npv", "accuracy", "f1", "f2", "f0_5", "p4", "phi"]

        if not isinstance(add_metrics, list):
            raise Exception(
                "add_metrics must be a list containing one or more of the following:",
                allowed,
            )

        if not all(metric in allowed for metric in add_metrics):
            raise ValueError(
                "Invalid metric. " f"Allowed metrics are: {', '.join(allowed)}."
            )

        df_truth_space = truth_space_table_from_labels_column(
            self._linker,
            labels_column_name,
            threshold_actual=threshold_actual,
            match_weight_round_to_nearest=match_weight_round_to_nearest,
            positives_not_captured_by_blocking_rules_scored_as_zero=positives_not_captured_by_blocking_rules_scored_as_zero,
        )
        recs = df_truth_space.as_record_dict()

        if output_type == "threshold_selection":
            return threshold_selection_tool(recs, add_metrics=add_metrics)
        elif output_type == "accuracy":
            return accuracy_chart(recs, add_metrics=add_metrics)
        elif output_type == "roc":
            return roc_chart(recs)
        elif output_type == "precision_recall":
            return precision_recall_chart(recs)
        elif output_type == "table":
            return df_truth_space
        else:
            raise ValueError(
                "Invalid chart_type. Allowed chart types are: "
                "'threshold_selection', 'roc', 'precision_recall', 'accuracy."
            )

    def accuracy_analysis_from_labels_table(
        self,
        labels_splinkdataframe_or_table_name: str | SplinkDataFrame,
        *,
        threshold_actual: float = 0.5,
        match_weight_round_to_nearest: float = 0.1,
        output_type: Literal[
            "threshold_selection", "roc", "precision_recall", "table", "accuracy"
        ] = "threshold_selection",
        add_metrics: List[
            Literal[
                "specificity",
                "npv",
                "accuracy",
                "f1",
                "f2",
                "f0_5",
                "p4",
                "phi",
            ]
        ] = [],
    ) -> Union[ChartReturnType, SplinkDataFrame]:
        """Generate an accuracy chart or table from labelled (ground truth) data.

        The table of labels should be in the following format, and should be registered
        as a table with your database using
        `labels_table = linker.register_labels_table(my_df)`

        |source_dataset_l|unique_id_l|source_dataset_r|unique_id_r|clerical_match_score|
        |----------------|-----------|----------------|-----------|--------------------|
        |df_1            |1          |df_2            |2          |0.99                |
        |df_1            |1          |df_2            |3          |0.2                 |

        Note that `source_dataset` and `unique_id` should correspond to the values
        specified in the settings dict, and the `input_table_aliases` passed to the
        `linker` object.

        For `dedupe_only` links, the `source_dataset` columns can be ommitted.

        Args:
            labels_splinkdataframe_or_table_name (str | SplinkDataFrame): Name of table
                containing labels in the database
            threshold_actual (float, optional): Where the `clerical_match_score`
                provided by the user is a probability rather than binary, this value
                is used as the threshold to classify `clerical_match_score`s as binary
                matches or non matches. Defaults to 0.5.
            match_weight_round_to_nearest (float, optional): When provided, thresholds
                are rounded.  When large numbers of labels are provided, this is
                sometimes necessary to reduce the size of the ROC table, and therefore
                the number of points plotted on the chart. Defaults to None.
            add_metrics (list(str), optional): Precision and recall metrics are always
                included. Where provided, `add_metrics` specifies additional metrics
                to show, with the following options:

                - `"specificity"`: specificity, selectivity, true negative rate (TNR)
                - `"npv"`: negative predictive value (NPV)
                - `"accuracy"`: overall accuracy (TP+TN)/(P+N)
                - `"f1"`/`"f2"`/`"f0_5"`: F-scores for \u03b2=1 (balanced), \u03b2=2
                (emphasis on recall) and \u03b2=0.5 (emphasis on precision)
                - `"p4"` -  an extended F1 score with specificity and NPV included
                - `"phi"` - \u03c6 coefficient or Matthews correlation coefficient (MCC)
        Examples:
            ```py
            linker.accuracy_analysis_from_labels_table("ground_truth", add_metrics=["f1"])
            ```

        Returns:
            altair.Chart: An altair chart
        """  # noqa: E501

        allowed = ["specificity", "npv", "accuracy", "f1", "f2", "f0_5", "p4", "phi"]

        if not isinstance(add_metrics, list):
            raise Exception(
                "add_metrics must be a list containing one or more of the following:",
                allowed,
            )

        if not all(metric in allowed for metric in add_metrics):
            raise ValueError(
                f"Invalid metric. Allowed metrics are: {', '.join(allowed)}."
            )

        labels_tablename = self._get_labels_tablename_from_input(
            labels_splinkdataframe_or_table_name
        )
        self._linker._raise_error_if_necessary_accuracy_columns_not_computed()
        df_truth_space = truth_space_table_from_labels_table(
            self._linker,
            labels_tablename,
            threshold_actual=threshold_actual,
            match_weight_round_to_nearest=match_weight_round_to_nearest,
        )
        recs = df_truth_space.as_record_dict()

        if output_type == "threshold_selection":
            return threshold_selection_tool(recs, add_metrics=add_metrics)
        elif output_type == "accuracy":
            return accuracy_chart(recs, add_metrics=add_metrics)
        elif output_type == "roc":
            return roc_chart(recs)
        elif output_type == "precision_recall":
            return precision_recall_chart(recs)
        elif output_type == "table":
            return df_truth_space
        else:
            raise ValueError(
                "Invalid chart_type. Allowed chart types are: "
                "'threshold_selection', 'roc', 'precision_recall', 'accuracy."
            )

    def prediction_errors_from_labels_column(
        self,
        label_colname,
        include_false_positives=True,
        include_false_negatives=True,
        threshold=0.5,
    ):
        """Generate a dataframe containing false positives and false negatives
        based on the comparison between the splink match probability and the
        labels column.  A label column is a column in the input dataset that contains
        the 'ground truth' cluster to which the record belongs

        Args:
            label_colname (str): Name of labels column in input data
            include_false_positives (bool, optional): Defaults to True.
            include_false_negatives (bool, optional): Defaults to True.
            threshold (float, optional): Threshold above which a score is considered
                to be a match. Defaults to 0.5.

        Returns:
            SplinkDataFrame:  Table containing false positives and negatives
        """
        return prediction_errors_from_label_column(
            self._linker,
            label_colname,
            include_false_positives,
            include_false_negatives,
            threshold,
        )
