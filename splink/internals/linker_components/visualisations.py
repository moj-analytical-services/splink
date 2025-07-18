from __future__ import annotations

from typing import TYPE_CHECKING, Any

from splink.internals.charts import (
    ChartReturnType,
    match_weights_histogram,
    parameter_estimate_comparisons,
    waterfall_chart,
)
from splink.internals.cluster_studio import (
    SamplingMethods,
    render_splink_cluster_studio_html,
)
from splink.internals.comparison_vector_distribution import (
    comparison_vector_distribution_sql,
)
from splink.internals.match_weights_histogram import histogram_data
from splink.internals.misc import ensure_is_list
from splink.internals.pipeline import CTEPipeline
from splink.internals.splink_comparison_viewer import (
    comparison_viewer_table_sqls,
    render_splink_comparison_viewer_html,
)
from splink.internals.splink_dataframe import SplinkDataFrame
from splink.internals.term_frequencies import (
    tf_adjustment_chart,
)

if TYPE_CHECKING:
    from splink.internals.linker import Linker


class LinkerVisualisations:
    """Visualisations to help you understand and diagnose your linkage model.
    Accessed via `linker.visualisations`.

    Most of the visualisations return an [altair.Chart](https://altair-viz.github.io/user_guide/generated/toplevel/altair.Chart.html)
    object, meaning it can be saved an manipulated using Altair.

    For example:

    ```py

    altair_chart = linker.visualisations.match_weights_chart()

    # Save to various formats
    altair_chart.save("mychart.png")
    altair_chart.save("mychart.html")
    altair_chart.save("mychart.svg")
    altair_chart.save("mychart.json")

    # Get chart spec as dict
    altair_chart.to_dict()
    ```


    To save the chart as a self-contained html file with all scripts
    inlined so it can be viewed offline:

    ```py
    from splink.internals.charts import save_offline_chart
    c = linker.visualisations.match_weights_chart()
    save_offline_chart(c.to_dict(), "test_chart.html")
    ```

    View resultant html file in Jupyter (or just load it in your browser)

    ```py
    from IPython.display import IFrame
    IFrame(src="./test_chart.html", width=1000, height=500)
    ```
    """

    def __init__(self, linker: Linker):
        self._linker = linker

    def match_weights_chart(self, as_dict: bool = False) -> ChartReturnType:
        """Display a chart of the (partial) match weights of the linkage model

        Args:
            as_dict (bool, optional): If True, return the chart as a dictionary.

        Examples:
            ```py
            altair_chart = linker.visualisations.match_weights_chart()
            altair_chart.save("mychart.png")
            ```
        Returns:
            altair_chart: An Altair chart
        """
        return self._linker._settings_obj.match_weights_chart(as_dict)

    def m_u_parameters_chart(self, as_dict: bool = False) -> ChartReturnType:
        """Display a chart of the m and u parameters of the linkage model

        Args:
            as_dict (bool, optional): If True, return the chart as a dictionary.

        Examples:
            ```py
            altair_chart = linker.visualisations.m_u_parameters_chart()
            altair_chart.save("mychart.png")
            ```

        Returns:
            altair_chart: An altair chart
        """

        return self._linker._settings_obj.m_u_parameters_chart(as_dict)

    def match_weights_histogram(
        self,
        df_predict: SplinkDataFrame,
        target_bins: int = 30,
        width: int = 600,
        height: int = 250,
        as_dict: bool = False,
    ) -> ChartReturnType:
        """Generate a histogram that shows the distribution of match weights in
        `df_predict`

        Args:
            df_predict (SplinkDataFrame): Output of `linker.inference.predict()`
            target_bins (int, optional): Target number of bins in histogram. Defaults to
                30.
            width (int, optional): Width of output. Defaults to 600.
            height (int, optional): Height of output chart. Defaults to 250.
            as_dict (bool, optional): If True, return the chart as a dictionary.

        Examples:
            ```py
            df_predict = linker.inference.predict(threshold_match_weight=-2)
            linker.visualisations.match_weights_histogram(df_predict)
            ```
        Returns:
            altair_chart: An Altair chart

        """
        df = histogram_data(self._linker, df_predict, target_bins)
        recs = df.as_record_dict()
        return match_weights_histogram(
            recs, width=width, height=height, as_dict=as_dict
        )

    def parameter_estimate_comparisons_chart(
        self, include_m: bool = True, include_u: bool = False, as_dict: bool = False
    ) -> ChartReturnType:
        """Show a chart that shows how parameter estimates have differed across
        the different estimation methods you have used.

        For example, if you have run two EM estimation sessions, blocking on
        different variables, and both result in parameter estimates for
        first_name, this chart will enable easy comparison of the different
        estimates

        Args:
            include_m (bool, optional): Show different estimates of m values. Defaults
                to True.
            include_u (bool, optional): Show different estimates of u values. Defaults
                to False.
            as_dict (bool, optional): If True, return the chart as a dictionary.

        Examples:
            ```py
            linker.training.estimate_parameters_using_expectation_maximisation(
                blocking_rule=block_on("first_name"),
            )

            linker.training.estimate_parameters_using_expectation_maximisation(
                blocking_rule=block_on("surname"),
            )

            linker.visualisations.parameter_estimate_comparisons_chart()
            ```

        Returns:
            altair_chart: An Altair chart

        """
        records = self._linker._settings_obj._parameter_estimates_as_records

        to_retain = []
        if include_m:
            to_retain.append("m")
        if include_u:
            to_retain.append("u")

        records = [r for r in records if r["m_or_u"] in to_retain]

        return parameter_estimate_comparisons(records, as_dict)

    def tf_adjustment_chart(
        self,
        output_column_name: str,
        n_most_freq: int = 10,
        n_least_freq: int = 10,
        vals_to_include: str | list[str] | None = None,
        as_dict: bool = False,
    ) -> ChartReturnType:
        """Display a chart showing the impact of term frequency adjustments on a
        specific comparison level.
        Each value

        Args:
            output_column_name (str): Name of an output column for which term frequency
                 adjustment has been applied.
            n_most_freq (int, optional): Number of most frequent values to show. If this
                 or `n_least_freq` set to None, all values will be shown.
                Default to 10.
            n_least_freq (int, optional): Number of least frequent values to show. If
                this or `n_most_freq` set to None, all values will be shown.
                Default to 10.
            vals_to_include (list, optional): Specific values for which to show term
                frequency adjustments.
                Defaults to None.
            as_dict (bool, optional): If True, return the chart as a dictionary.

        Examples:
            ```py
            linker.visualisations.tf_adjustment_chart("first_name")
            ```

        Returns:
            altair_chart: An Altair chart
        """

        # Comparisons with TF adjustments
        tf_comparisons = [
            c.output_column_name
            for c in self._linker._settings_obj.comparisons
            if any([cl._has_tf_adjustments for cl in c.comparison_levels])
        ]
        if output_column_name not in tf_comparisons:
            raise ValueError(
                f"{output_column_name} is not a valid comparison column, or does not"
                f" have term frequency adjustment activated"
            )

        vals_to_include = (
            [] if vals_to_include is None else ensure_is_list(vals_to_include)
        )

        return tf_adjustment_chart(
            self._linker,
            output_column_name,
            n_most_freq,
            n_least_freq,
            vals_to_include,
            as_dict,
        )

    def waterfall_chart(
        self,
        records: list[dict[str, Any]],
        filter_nulls: bool = True,
        remove_sensitive_data: bool = False,
        as_dict: bool = False,
    ) -> ChartReturnType:
        """Visualise how the final match weight is computed for the provided pairwise
        record comparisons.

        Records must be provided as a list of dictionaries. This would usually be
        obtained from `df.as_record_dict(limit=n)` where `df` is a SplinkDataFrame.

        Examples:
            ```py
            df = linker.inference.predict(threshold_match_weight=2)
            records = df.as_record_dict(limit=10)
            linker.visualisations.waterfall_chart(records)
            ```

        Args:
            records (List[dict]): Usually be obtained from `df.as_record_dict(limit=n)`
                where `df` is a SplinkDataFrame.
            filter_nulls (bool, optional): Whether the visualisation shows null
                comparisons, which have no effect on final match weight. Defaults to
                True.
            remove_sensitive_data (bool, optional): When True, The waterfall chart will
                contain match weights only, and all of the (potentially sensitive) data
                from the input tables will be removed prior to the chart being created.
            as_dict (bool, optional): If True, return the chart as a dictionary.


        Returns:
            altair_chart: An Altair chart

        """
        self._linker._raise_error_if_necessary_waterfall_columns_not_computed()

        return waterfall_chart(
            records,
            self._linker._settings_obj,
            filter_nulls,
            remove_sensitive_data,
            as_dict,
        )

    def comparison_viewer_dashboard(
        self,
        df_predict: SplinkDataFrame,
        out_path: str,
        overwrite: bool = False,
        num_example_rows: int = 2,
        minimum_comparison_vector_count: int = 0,
        return_html_as_string: bool = False,
    ) -> str | None:
        """Generate an interactive html visualization of the linker's predictions and
        save to `out_path`.  For more information see
        [this video](https://www.youtube.com/watch?v=DNvCMqjipis)


        Args:
            df_predict (SplinkDataFrame): The outputs of `linker.inference.predict()`
            out_path (str): The path (including filename) to save the html file to.
            overwrite (bool, optional): Overwrite the html file if it already exists?
                Defaults to False.
            num_example_rows (int, optional): Number of example rows per comparison
                vector. Defaults to 2.
            minimum_comparison_vector_count (int, optional): The minimum number
                of times that a comparison vector has to occur for it to
                be included in the dashboard. This can reduce the size of
                the produced html file by eliminating the rarest comparison
                vectors. Defaults to 0 (all comparison vectors are included).
            return_html_as_string: If True, return the html as a string

        Examples:
            ```py
            df_predictions = linker.inference.predict()
            linker.visualisations.comparison_viewer_dashboard(
                df_predictions, "scv.html", True, 2
            )
            ```

            Optionally, in Jupyter, you can display the results inline
            Otherwise you can just load the html file in your browser

            ```py
            from IPython.display import IFrame
            IFrame(src="./scv.html", width="100%", height=1200)
            ```

        """
        self._linker._raise_error_if_necessary_waterfall_columns_not_computed()
        pipeline = CTEPipeline([df_predict])
        sql = comparison_vector_distribution_sql(self._linker)
        pipeline.enqueue_sql(sql, "__splink__df_comparison_vector_distribution")

        sqls = comparison_viewer_table_sqls(
            self._linker,
            num_example_rows,
            minimum_comparison_vector_count,
        )
        pipeline.enqueue_list_of_sqls(sqls)

        df = self._linker._db_api.sql_pipeline_to_splink_dataframe(pipeline)

        rendered = render_splink_comparison_viewer_html(
            df.as_record_dict(),
            self._linker._settings_obj._as_completed_dict(),
            out_path,
            overwrite,
        )
        if return_html_as_string:
            return rendered
        return None

    def cluster_studio_dashboard(
        self,
        df_predict: SplinkDataFrame,
        df_clustered: SplinkDataFrame,
        out_path: str,
        sampling_method: SamplingMethods = "random",
        sample_size: int = 10,
        cluster_ids: list[str] = None,
        cluster_names: list[str] = None,
        overwrite: bool = False,
        return_html_as_string: bool = False,
        _df_cluster_metrics: SplinkDataFrame = None,
    ) -> str | None:
        """Generate an interactive html visualization of the predicted cluster and
        save to `out_path`.

        Args:
            df_predict (SplinkDataFrame): The outputs of `linker.inference.predict()`
            df_clustered (SplinkDataFrame): The outputs of
                `linker.clustering.cluster_pairwise_predictions_at_threshold()`
            out_path (str): The path (including filename) to save the html file to.
            sampling_method (str, optional): `random`, `by_cluster_size` or
                `lowest_density_clusters_by_size`. Defaults to `random`.
            sample_size (int, optional): Number of clusters to show in the dahboard.
                Defaults to 10.
            cluster_ids (list): The IDs of the clusters that will be displayed in the
                dashboard.  If provided, ignore the `sampling_method` and `sample_size`
                arguments. Defaults to None.
            overwrite (bool, optional): Overwrite the html file if it already exists?
                Defaults to False.
            cluster_names (list, optional): If provided, the dashboard will display
                these names in the selection box. Ony works in conjunction with
                `cluster_ids`.  Defaults to None.
            return_html_as_string: If True, return the html as a string

        Examples:
            ```py
            df_p = linker.inference.predict()
            df_c = linker.visualisations.cluster_pairwise_predictions_at_threshold(
                df_p, 0.5
            )

            linker.visualisations.cluster_studio_dashboard(
                df_p, df_c, [0, 4, 7], "cluster_studio.html"
            )
            ```

            Optionally, in Jupyter, you can display the results inline
            Otherwise you can just load the html file in your browser

            ```py
            from IPython.display import IFrame
            IFrame(src="./cluster_studio.html", width="100%", height=1200)
            ```
        """
        self._linker._raise_error_if_necessary_waterfall_columns_not_computed()

        rendered = render_splink_cluster_studio_html(
            self._linker,
            df_predict,
            df_clustered,
            out_path,
            sampling_method=sampling_method,
            sample_size=sample_size,
            cluster_ids=cluster_ids,
            overwrite=overwrite,
            cluster_names=cluster_names,
            _df_cluster_metrics=_df_cluster_metrics,
        )

        if return_html_as_string:
            return rendered
        return None
