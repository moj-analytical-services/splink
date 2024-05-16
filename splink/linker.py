from __future__ import annotations

import json
import logging
import os
from copy import copy, deepcopy
from pathlib import Path
from statistics import median
from typing import Any, Dict, List, Optional, Sequence, Union

from .accuracy import (
    prediction_errors_from_label_column,
    prediction_errors_from_labels_table,
    truth_space_table_from_labels_column,
    truth_space_table_from_labels_table,
)
from .blocking import (
    BlockingRule,
    SaltedBlockingRule,
    block_using_rules_sqls,
    blocking_rule_to_obj,
    materialise_exploded_id_tables,
)
from .blocking_rule_creator import BlockingRuleCreator
from .blocking_rule_creator_utils import to_blocking_rule_creator
from .cache_dict_with_logging import CacheDictWithLogging
from .charts import (
    ChartReturnType,
    accuracy_chart,
    match_weights_histogram,
    parameter_estimate_comparisons,
    precision_recall_chart,
    roc_chart,
    threshold_selection_tool,
    unlinkables_chart,
    waterfall_chart,
)
from .cluster_studio import SamplingMethods, render_splink_cluster_studio_html
from .comparison import Comparison
from .comparison_level import ComparisonLevel
from .comparison_vector_distribution import (
    comparison_vector_distribution_sql,
)
from .comparison_vector_values import compute_comparison_vector_values_sql
from .connected_components import (
    _cc_create_unique_id_cols,
    solve_connected_components,
)
from .database_api import AcceptableInputTableType, DatabaseAPISubClass
from .dialects import SplinkDialect
from .edge_metrics import compute_edge_metrics
from .em_training_session import EMTrainingSession
from .estimate_u import estimate_u_values
from .exceptions import SplinkException
from .find_brs_with_comparison_counts_below_threshold import (
    find_blocking_rules_below_threshold_comparison_count,
)
from .find_matches_to_new_records import add_unique_id_and_source_dataset_cols_if_needed
from .graph_metrics import (
    GraphMetricsResults,
    _node_degree_sql,
    _size_density_centralisation_sql,
)
from .input_column import InputColumn
from .internals.blocking_analysis import (
    _cumulative_comparisons_to_be_scored_from_blocking_rules,
)
from .labelling_tool import (
    generate_labelling_tool_comparisons,
    render_labelling_tool_html,
)
from .m_from_labels import estimate_m_from_pairwise_labels
from .m_training import estimate_m_values_from_label_column
from .match_weights_histogram import histogram_data
from .misc import (
    ascii_uid,
    bayes_factor_to_prob,
    ensure_is_iterable,
    ensure_is_list,
    prob_to_bayes_factor,
)
from .optimise_cost_of_brs import suggest_blocking_rules
from .pipeline import CTEPipeline
from .predict import (
    predict_from_comparison_vectors_sqls,
    predict_from_comparison_vectors_sqls_using_settings,
)
from .settings_creator import SettingsCreator
from .settings_validation.log_invalid_columns import (
    InvalidColumnsLogger,
    SettingsColumnCleaner,
)
from .settings_validation.valid_types import (
    _validate_dialect,
)
from .splink_comparison_viewer import (
    comparison_viewer_table_sqls,
    render_splink_comparison_viewer_html,
)
from .splink_dataframe import SplinkDataFrame
from .term_frequencies import (
    _join_new_table_to_df_concat_with_tf_sql,
    colname_to_tf_tablename,
    term_frequencies_for_single_column_sql,
    tf_adjustment_chart,
)
from .unique_id_concat import (
    _composite_unique_id_from_edges_sql,
    _composite_unique_id_from_nodes_sql,
)
from .unlinkables import unlinkables_data
from .vertically_concatenate import (
    compute_df_concat_with_tf,
    enqueue_df_concat,
    enqueue_df_concat_with_tf,
    split_df_concat_with_tf_into_two_tables_sqls,
)

logger = logging.getLogger(__name__)


class Linker:
    """The Linker object manages the data linkage process and holds the data linkage
    model.

    Most of Splink's functionality can  be accessed by calling methods (functions)
    on the linker, such as `linker.predict()`, `linker.profile_columns()` etc.

    The Linker class is intended for subclassing for specific backends, e.g.
    a `DuckDBLinker`.
    """

    def __init__(
        self,
        input_table_or_tables: str | list[str],
        settings: SettingsCreator | dict[str, Any] | Path | str,
        database_api: DatabaseAPISubClass,
        set_up_basic_logging: bool = True,
        input_table_aliases: str | list[str] | None = None,
        validate_settings: bool = True,
    ):
        """
        Initialise the linker object, which manages the data linkage process and
        holds the data linkage model.

        Examples:

            Dedupe
            ```py
            linker = Linker(df, settings_dict, db_api)
            ```
            Link
            ```py
            df_1 = pd.read_parquet("table_1/")
            df_2 = pd.read_parquet("table_2/")
            linker = Linker(
                [df_1, df_2],
                settings_dict,
                input_table_aliases=["customers", "contact_center_callers"]
                )
            ```
            Dedupe with a pre-trained model read from a json file
            ```py
            df = pd.read_csv("data_to_dedupe.csv")
            linker = Linker(df, "model.json")
            ```

        Args:
            input_table_or_tables (Union[str, list]): Input data into the linkage model.
                Either a single string (the name of a table in a database) for
                deduplication jobs, or a list of strings  (the name of tables in a
                database) for link_only or link_and_dedupe.  For some linkers, such as
                the DuckDBLinker and the SparkLinker, it's also possible to pass in
                dataframes (Pandas and Spark respectively) rather than strings.
            settings_dict (dict | Path, optional): A Splink settings dictionary, or a
                path to a json defining a settingss dictionary or pre-trained model.
                If not provided when the object is created, can later be added using
                `linker.load_settings()` or `linker.load_model()` Defaults to None.
            set_up_basic_logging (bool, optional): If true, sets ups up basic logging
                so that Splink sends messages at INFO level to stdout. Defaults to True.
            input_table_aliases (Union[str, list], optional): Labels assigned to
                input tables in Splink outputs.  If the names of the tables in the
                input database are long or unspecific, this argument can be used
                to attach more easily readable/interpretable names. Defaults to None.
            validate_settings (bool, optional): When True, check your settings
                dictionary for any potential errors that may cause splink to fail.
        """
        self._db_schema = "splink"
        if set_up_basic_logging:
            logging.basicConfig(
                format="%(message)s",
            )
            splink_logger = logging.getLogger("splink")
            splink_logger.setLevel(logging.INFO)

        self.db_api = database_api

        # TODO: temp hack for compat
        self._intermediate_table_cache: CacheDictWithLogging = (
            self.db_api._intermediate_table_cache
        )

        # Turn into a creator
        if not isinstance(settings, SettingsCreator):
            settings_creator = SettingsCreator.from_path_or_dict(settings)
        else:
            settings_creator = settings

        # Deal with uuid
        if settings_creator.linker_uid is None:
            settings_creator.linker_uid = ascii_uid(8)

        # Do we trust the dialect set in the settings dict
        # or overwrite it with the db api dialect?
        # Maybe overwrite it here and incompatibilities have to be dealt with
        # by comparisons/ blocking rules etc??
        self._settings_obj = settings_creator.get_settings(
            database_api.sql_dialect.name
        )

        # TODO: Add test of what happens if the db_api is for a different backend
        # to the sql_dialect set in the settings dict

        self._input_tables_dict = self._register_input_tables(
            input_table_or_tables,
            input_table_aliases,
        )

        self._validate_input_dfs()
        self._validate_settings(validate_settings)
        self._em_training_sessions: list[EMTrainingSession] = []

        self.debug_mode = False

    def _input_columns(
        self,
        include_unique_id_col_names: bool = True,
        include_additional_columns_to_retain: bool = True,
    ) -> list[InputColumn]:
        """Retrieve the column names from the input dataset(s) as InputColumns

        Args:
            include_unique_id_col_names (bool, optional): Whether to include unique ID
                column names. Defaults to True.
            include_additional_columns_to_retain (bool, optional): Whether to include
                additional columns to retain. Defaults to True.

        Raises:
            SplinkException: If the input frames have different sets of columns.

        Returns:
            list[InputColumn]
        """

        input_dfs = self._input_tables_dict.values()

        # get a list of the column names for each input frame
        # sort it for consistent ordering, and give each frame's
        # columns as a tuple so we can hash it
        column_names_by_input_df = [
            tuple(sorted([col.name for col in input_df.columns]))
            for input_df in input_dfs
        ]
        # check that the set of input columns is the same for each frame,
        # fail if the sets are different
        if len(set(column_names_by_input_df)) > 1:
            common_cols = set.intersection(
                *(set(col_names) for col_names in column_names_by_input_df)
            )
            problem_names = {
                col
                for frame_col_names in column_names_by_input_df
                for col in frame_col_names
                if col not in common_cols
            }
            raise SplinkException(
                "All linker input frames must have the same set of columns.  "
                "The following columns were not found in all input frames: "
                + ", ".join(problem_names)
            )

        columns = next(iter(input_dfs)).columns

        remove_columns = []
        if not include_unique_id_col_names:
            remove_columns.extend(
                self._settings_obj.column_info_settings.unique_id_input_columns
            )
        if not include_additional_columns_to_retain:
            remove_columns.extend(self._settings_obj._additional_columns_to_retain)

        remove_id_cols = [c.unquote().name for c in remove_columns]
        columns = [col for col in columns if col.unquote().name not in remove_id_cols]

        return columns

    @property
    def _source_dataset_column_already_exists(self):
        input_cols = [c.unquote().name for c in self._input_columns()]
        return (
            self._settings_obj.column_info_settings.source_dataset_column_name
            in input_cols
        )

    @property
    def _cache_uid(self):
        return self._settings_obj._cache_uid

    @_cache_uid.setter
    def _cache_uid(self, value):
        self._settings_obj._cache_uid = value

    @property
    def _two_dataset_link_only(self):
        # Two dataset link only join is a special case where an inner join of the
        # two datasets is much more efficient than self-joining the vertically
        # concatenation of all input datasets

        if (
            len(self._input_tables_dict) == 2
            and self._settings_obj._link_type == "link_only"
        ):
            return True
        else:
            return False

    # convenience wrappers:
    @property
    def debug_mode(self) -> bool:
        return self.db_api.debug_mode

    @debug_mode.setter
    def debug_mode(self, value: bool) -> None:
        self.db_api.debug_mode = value

    # TODO: rename these!
    @property
    def _sql_dialect(self) -> str:
        return self.db_api.sql_dialect.name

    @property
    def _sql_dialect_object(self) -> SplinkDialect:
        return self.db_api.sql_dialect

    @property
    def _infinity_expression(self):
        return self._sql_dialect_object.infinity_expression

    def _random_sample_sql(
        self, proportion, sample_size, seed=None, table=None, unique_id=None
    ):
        return self._sql_dialect_object.random_sample_sql(
            proportion, sample_size, seed=seed, table=table, unique_id=unique_id
        )

    def _register_input_tables(
        self,
        input_tables: Sequence[AcceptableInputTableType],
        input_aliases: Optional[str | List[str]],
    ) -> Dict[str, SplinkDataFrame]:
        if input_aliases is None:
            input_table_aliases = [
                f"__splink__input_table_{i}" for i, _ in enumerate(input_tables)
            ]
            overwrite = True
        else:
            input_table_aliases = ensure_is_list(input_aliases)
            overwrite = False

        return self.db_api.register_multiple_tables(
            input_tables, input_table_aliases, overwrite
        )

    def _check_for_valid_settings(self):
        # raw tables don't yet exist in db
        return hasattr(self, "_input_tables_dict")

    def _validate_settings(self, validate_settings):
        # Vaidate our settings after plugging them through
        # `Settings(<settings>)`
        if not self._check_for_valid_settings():
            return

        # Run miscellaneous checks on our settings dictionary.
        _validate_dialect(
            settings_dialect=self._settings_obj._sql_dialect,
            linker_dialect=self._sql_dialect,
            linker_type=self.__class__.__name__,
        )

        # Constructs output logs for our various settings inputs
        cleaned_settings = SettingsColumnCleaner(
            settings_object=self._settings_obj,
            input_columns=self._input_tables_dict,
        )
        InvalidColumnsLogger(cleaned_settings).construct_output_logs(validate_settings)

    def _table_to_splink_dataframe(
        self, templated_name: str, physical_name: str
    ) -> SplinkDataFrame:
        """Create a SplinkDataframe from a table in the underlying database called
        `physical_name`.
        Associate a `templated_name` with this table, which signifies the purpose
        or 'meaning' of this table to splink. (e.g. `__splink__df_blocked`)
        Args:
            templated_name (str): The purpose of the table to Splink
            physical_name (str): The name of the table in the underlying databse
        """
        return self.db_api.table_to_splink_dataframe(templated_name, physical_name)

    def register_table(
        self,
        input_table: AcceptableInputTableType,
        table_name: str,
        overwrite: bool = False,
    ) -> SplinkDataFrame:
        """
        Register a table to your backend database, to be used in one of the
        splink methods, or simply to allow querying.

        Tables can be of type: dictionary, record level dictionary,
        pandas dataframe, pyarrow table and in the spark case, a spark df.

        Examples:
            ```py
            test_dict = {"a": [666,777,888],"b": [4,5,6]}
            linker.register_table(test_dict, "test_dict")
            linker.query_sql("select * from test_dict")
            ```

        Args:
            input: The data you wish to register. This can be either a dictionary,
                pandas dataframe, pyarrow table or a spark dataframe.
            table_name (str): The name you wish to assign to the table.
            overwrite (bool): Overwrite the table in the underlying database if it
                exists

        Returns:
            SplinkDataFrame: An abstraction representing the table created by the sql
                pipeline
        """

        return self.db_api.register_table(input_table, table_name, overwrite)

    def query_sql(self, sql, output_type="pandas"):
        """
        Run a SQL query against your backend database and return
        the resulting output.

        Examples:
            ```py
            linker = Linker(df, settings, db_api)
            df_predict = linker.predict()
            linker.query_sql(f"select * from {df_predict.physical_name} limit 10")
            ```

        Args:
            sql (str): The SQL to be queried.
            output_type (str): One of splink_df/splinkdf or pandas.
                This determines the type of table that your results are output in.
        """

        output_tablename_templated = "__splink__df_sql_query"

        pipeline = CTEPipeline()
        pipeline.enqueue_sql(sql, output_tablename_templated)
        splink_dataframe = self.db_api.sql_pipeline_to_splink_dataframe(
            pipeline, use_cache=False
        )

        if output_type in ("splink_df", "splinkdf"):
            return splink_dataframe
        elif output_type == "pandas":
            out = splink_dataframe.as_pandas_dataframe()
            # If pandas, drop the table to cleanup the db
            splink_dataframe.drop_table_from_database_and_remove_from_cache()
            return out
        else:
            raise ValueError(
                f"output_type '{output_type}' is not supported.",
                "Must be one of 'splink_df'/'splinkdf' or 'pandas'",
            )

    def __deepcopy__(self, memo):
        """When we do EM training, we need a copy of the linker which is independent
        of the main linker e.g. setting parameters on the copy will not affect the
        main linker.  This method implements ensures linker can be deepcopied.
        """
        new_linker = copy(self)
        new_linker._em_training_sessions = []
        new_settings = deepcopy(self._settings_obj)
        new_linker._settings_obj = new_settings
        return new_linker

    def _predict_warning(self):
        if not self._settings_obj._is_fully_trained:
            msg = (
                "\n -- WARNING --\n"
                "You have called predict(), but there are some parameter "
                "estimates which have neither been estimated or specified in your "
                "settings dictionary.  To produce predictions the following"
                " untrained trained parameters will use default values."
            )
            messages = self._settings_obj._not_trained_messages()

            warn_message = "\n".join([msg] + messages)

            logger.warning(warn_message)

    def _validate_input_dfs(self):
        if not hasattr(self, "_input_tables_dict"):
            # This is only triggered where a user loads a settings dict from a
            # given file path.
            return

        for df in self._input_tables_dict.values():
            df.validate()

        if self._settings_obj._link_type == "dedupe_only":
            if len(self._input_tables_dict) > 1:
                raise ValueError(
                    'If link_type = "dedupe only" then input tables must contain '
                    "only a single input table",
                )

    def _populate_probability_two_random_records_match_from_trained_values(self):
        recip_prop_matches_estimates = []

        logger.log(
            15,
            (
                "---- Using training sessions to compute "
                "probability two random records match ----"
            ),
        )
        for em_training_session in self._em_training_sessions:
            training_lambda = em_training_session.core_model_settings.probability_two_random_records_match  # noqa: E501
            training_lambda_bf = prob_to_bayes_factor(training_lambda)
            reverse_level_infos = (
                em_training_session._comparison_levels_to_reverse_blocking_rule
            )

            logger.log(
                15,
                "\n"
                f"Probability two random records match from trained model blocking on "
                f"{em_training_session._blocking_rule_for_training.blocking_rule_sql}: "
                f"{training_lambda:,.3f}",
            )

            for reverse_level_info in reverse_level_infos:
                # Get comparison level on current settings obj
                # TODO: do we need this dance? We already have the level + comparison
                # maybe they are different copies with different values?
                reverse_level = reverse_level_info["level"]
                cc = self._settings_obj._get_comparison_by_output_column_name(
                    reverse_level_info["comparison"].output_column_name
                )

                cl = cc._get_comparison_level_by_comparison_vector_value(
                    reverse_level.comparison_vector_value
                )

                if cl._has_estimated_values:
                    bf = cl._trained_m_median / cl._trained_u_median
                else:
                    bf = cl._bayes_factor

                logger.log(
                    15,
                    f"Reversing comparison level {cc.output_column_name}"
                    f" using bayes factor {bf:,.3f}",
                )

                training_lambda_bf = training_lambda_bf / bf

                as_prob = bayes_factor_to_prob(training_lambda_bf)

                logger.log(
                    15,
                    (
                        "This estimate of probability two random records match now: "
                        f" {as_prob:,.3f} "
                        f"with reciprocal {(1/as_prob):,.3f}"
                    ),
                )
            logger.log(15, "\n---------")
            p = bayes_factor_to_prob(training_lambda_bf)
            recip_prop_matches_estimates.append(1 / p)

        prop_matches_estimate = 1 / median(recip_prop_matches_estimates)

        self._settings_obj._probability_two_random_records_match = prop_matches_estimate
        logger.log(
            15,
            "\nMedian of prop of matches estimates: "
            f"{self._settings_obj._probability_two_random_records_match:,.3f} "
            "reciprocal "
            f"{1/self._settings_obj._probability_two_random_records_match:,.3f}",
        )

    def _populate_m_u_from_trained_values(self):
        ccs = self._settings_obj.comparisons

        for cc in ccs:
            for cl in cc._comparison_levels_excluding_null:
                if cl._has_estimated_u_values:
                    cl.u_probability = cl._trained_u_median
                if cl._has_estimated_m_values:
                    cl.m_probability = cl._trained_m_median

    def delete_tables_created_by_splink_from_db(self):
        self.db_api.delete_tables_created_by_splink_from_db()

    def _raise_error_if_necessary_waterfall_columns_not_computed(self):
        ricc = self._settings_obj._retain_intermediate_calculation_columns
        rmc = self._settings_obj._retain_matching_columns
        if not (ricc and rmc):
            raise ValueError(
                "retain_intermediate_calculation_columns and "
                "retain_matching_columns must both be set to True in your settings"
                " dictionary to use this function, because otherwise the necessary "
                "columns will not be available in the input records."
                f" Their current values are {ricc} and {rmc}, respectively. "
                "Please re-run your linkage with them both set to True."
            )

    def _raise_error_if_necessary_accuracy_columns_not_computed(self):
        rmc = self._settings_obj._retain_matching_columns
        if not (rmc):
            raise ValueError(
                "retain_matching_columns must be set to True in your settings"
                " dictionary to use this function, because otherwise the necessary "
                "columns will not be available in the input records."
                f" Its current value is {rmc}. "
                "Please re-run your linkage with it set to True."
            )

    def compute_tf_table(self, column_name: str) -> SplinkDataFrame:
        """Compute a term frequency table for a given column and persist to the database

        This method is useful if you want to pre-compute term frequency tables e.g.
        so that real time linkage executes faster, or so that you can estimate
        various models without having to recompute term frequency tables each time

        Examples:

            Real time linkage
            ```py
            linker = Linker(df, db_api)
            linker.load_settings("saved_settings.json")
            linker.compute_tf_table("surname")
            linker.compare_two_records(record_left, record_right)
            ```
            Pre-computed term frequency tables
            ```py
            linker = Linker(df, db_api)
            df_first_name_tf = linker.compute_tf_table("first_name")
            df_first_name_tf.write.parquet("folder/first_name_tf")
            >>>
            # On subsequent data linking job, read this table rather than recompute
            df_first_name_tf = pd.read_parquet("folder/first_name_tf")
            df_first_name_tf.createOrReplaceTempView("__splink__df_tf_first_name")
            ```


        Args:
            column_name (str): The column name in the input table

        Returns:
            SplinkDataFrame: The resultant table as a splink data frame
        """

        input_col = InputColumn(
            column_name,
            column_info_settings=self._settings_obj.column_info_settings,
            sql_dialect=self._settings_obj._sql_dialect,
        )
        tf_tablename = colname_to_tf_tablename(input_col)
        cache = self._intermediate_table_cache

        if tf_tablename in cache:
            tf_df = cache.get_with_logging(tf_tablename)
        else:
            pipeline = CTEPipeline()
            pipeline = enqueue_df_concat(self, pipeline)
            sql = term_frequencies_for_single_column_sql(input_col)
            pipeline.enqueue_sql(sql, tf_tablename)
            tf_df = self.db_api.sql_pipeline_to_splink_dataframe(pipeline)
            self._intermediate_table_cache[tf_tablename] = tf_df

        return tf_df

    def deterministic_link(self) -> SplinkDataFrame:
        """Uses the blocking rules specified by
        `blocking_rules_to_generate_predictions` in the settings dictionary to
        generate pairwise record comparisons.

        For deterministic linkage, this should be a list of blocking rules which
        are strict enough to generate only true links.

        Deterministic linkage, however, is likely to result in missed links
        (false negatives).

        Examples:

            ```py
            from splink.linker import Linker
            from splink.duckdb.database_api import DuckDBAPI

            db_api = DuckDBAPI()

            settings = {
                "link_type": "dedupe_only",
                "blocking_rules_to_generate_predictions": [
                    "l.first_name = r.first_name",
                    "l.surname = r.surname",
                ],
                "comparisons": []
            }
            >>>
            linker = Linker(df, settings, db_api)
            df = linker.deterministic_link()
            ```


        Returns:
            SplinkDataFrame: A SplinkDataFrame of the pairwise comparisons.  This
                represents a table materialised in the database. Methods on the
                SplinkDataFrame allow you to access the underlying data.
        """
        pipeline = CTEPipeline()
        # Allows clustering during a deterministic linkage.
        # This is used in `cluster_pairwise_predictions_at_threshold`
        # to set the cluster threshold to 1

        df_concat_with_tf = compute_df_concat_with_tf(self, pipeline)
        pipeline = CTEPipeline([df_concat_with_tf])
        link_type = self._settings_obj._link_type

        blocking_input_tablename_l = "__splink__df_concat_with_tf"
        blocking_input_tablename_r = "__splink__df_concat_with_tf"

        link_type = self._settings_obj._link_type
        if (
            len(self._input_tables_dict) == 2
            and self._settings_obj._link_type == "link_only"
        ):
            sqls = split_df_concat_with_tf_into_two_tables_sqls(
                "__splink__df_concat_with_tf",
                self._settings_obj.column_info_settings.source_dataset_column_name,
            )
            pipeline.enqueue_list_of_sqls(sqls)

            blocking_input_tablename_l = "__splink__df_concat_with_tf_left"
            blocking_input_tablename_r = "__splink__df_concat_with_tf_right"
            link_type = "two_dataset_link_only"

        exploding_br_with_id_tables = materialise_exploded_id_tables(
            link_type=link_type,
            blocking_rules=self._settings_obj._blocking_rules_to_generate_predictions,
            db_api=self.db_api,
            splink_df_dict=self._input_tables_dict,
            source_dataset_input_column=self._settings_obj.column_info_settings.source_dataset_input_column,
            unique_id_input_column=self._settings_obj.column_info_settings.unique_id_input_column,
        )

        columns_to_select = self._settings_obj._columns_to_select_for_blocking
        sql_select_expr = ", ".join(columns_to_select)

        sqls = block_using_rules_sqls(
            input_tablename_l=blocking_input_tablename_l,
            input_tablename_r=blocking_input_tablename_r,
            blocking_rules=self._settings_obj._blocking_rules_to_generate_predictions,
            link_type=link_type,
            columns_to_select_sql=sql_select_expr,
            source_dataset_input_column=self._settings_obj.column_info_settings.source_dataset_input_column,
            unique_id_input_column=self._settings_obj.column_info_settings.unique_id_input_column,
        )
        pipeline.enqueue_list_of_sqls(sqls)

        deterministic_link_df = self.db_api.sql_pipeline_to_splink_dataframe(pipeline)
        deterministic_link_df.metadata["is_deterministic_link"] = True

        [b.drop_materialised_id_pairs_dataframe() for b in exploding_br_with_id_tables]

        return deterministic_link_df

    def estimate_u_using_random_sampling(
        self, max_pairs: float = 1e6, seed: int = None
    ) -> None:
        """Estimate the u parameters of the linkage model using random sampling.

        The u parameters represent the proportion of record comparisons that fall
        into each comparison level amongst truly non-matching records.

        This procedure takes a sample of the data and generates the cartesian
        product of pairwise record comparisons amongst the sampled records.
        The validity of the u values rests on the assumption that the resultant
        pairwise comparisons are non-matches (or at least, they are very unlikely to be
        matches). For large datasets, this is typically true.

        The results of estimate_u_using_random_sampling, and therefore an entire splink
        model, can be made reproducible by setting the seed parameter. Setting the seed
        will have performance implications as additional processing is required.

        Args:
            max_pairs (int): The maximum number of pairwise record comparisons to
            sample. Larger will give more accurate estimates
            but lead to longer runtimes.  In our experience at least 1e9 (one billion)
            gives best results but can take a long time to compute. 1e7 (ten million)
            is often adequate whilst testing different model specifications, before
            the final model is estimated.
            seed (int): Seed for random sampling. Assign to get reproducible u
            probabilities. Note, seed for random sampling is only supported for
            DuckDB and Spark, for Athena and SQLite set to None.

        Examples:
            ```py
            linker.estimate_u_using_random_sampling(1e8)
            ```

        Returns:
            None: Updates the estimated u parameters within the linker object
            and returns nothing.
        """
        if max_pairs == 1e6:
            # keep default value small so as not to take too long, but warn users
            logger.warning(
                "You are using the default value for `max_pairs`, "
                "which may be too small and thus lead to inaccurate estimates for your "
                "model's u-parameters. Consider increasing to 1e8 or 1e9, which will "
                "result in more accurate estimates, but with a longer run time."
            )
        estimate_u_values(self, max_pairs, seed)
        self._populate_m_u_from_trained_values()

        self._settings_obj._columns_without_estimated_parameters_message()

    def estimate_m_from_label_column(self, label_colname: str) -> None:
        """Estimate the m parameters of the linkage model from a label (ground truth)
        column in the input dataframe(s).

        The m parameters represent the proportion of record comparisons that fall
        into each comparison level amongst truly matching records.

        The ground truth column is used to generate pairwise record comparisons
        which are then assumed to be matches.

        For example, if the entity being matched is persons, and your input dataset(s)
        contain social security number, this could be used to estimate the m values
        for the model.

        Note that this column does not need to be fully populated.  A common case is
        where a unique identifier such as social security number is only partially
        populated.

        Args:
            label_colname (str): The name of the column containing the ground truth
                label in the input data.

        Examples:
            ```py
            linker.estimate_m_from_label_column("social_security_number")
            ```

        Returns:
            Updates the estimated m parameters within the linker object
            and returns nothing.
        """

        # Ensure this has been run on the main linker so that it can be used by
        # training linker when it checks the cache
        pipeline = CTEPipeline()
        compute_df_concat_with_tf(self, pipeline)

        estimate_m_values_from_label_column(
            self,
            self._input_tables_dict,
            label_colname,
        )
        self._populate_m_u_from_trained_values()

        self._settings_obj._columns_without_estimated_parameters_message()

    def estimate_parameters_using_expectation_maximisation(
        self,
        blocking_rule: Union[str, BlockingRuleCreator],
        comparisons_to_deactivate: list[Comparison] = None,
        comparison_levels_to_reverse_blocking_rule: list[ComparisonLevel] = None,
        estimate_without_term_frequencies: bool = False,
        fix_probability_two_random_records_match: bool = False,
        fix_m_probabilities: bool = False,
        fix_u_probabilities: bool = True,
        populate_probability_two_random_records_match_from_trained_values: bool = False,
    ) -> EMTrainingSession:
        """Estimate the parameters of the linkage model using expectation maximisation.

        By default, the m probabilities are estimated, but not the u probabilities,
        because good estimates for the u probabilities can be obtained from
        `linker.estimate_u_using_random_sampling()`.  You can change this by setting
        `fix_u_probabilities` to False.

        The blocking rule provided is used to generate pairwise record comparisons.
        Usually, this should be a blocking rule that results in a dataframe where
        matches are between about 1% and 99% of the comparisons.

        By default, m parameters are estimated for all comparisons except those which
        are included in the blocking rule.

        For example, if the blocking rule is `l.first_name = r.first_name`, then
        parameter esimates will be made for all comparison except those which use
        `first_name` in their sql_condition

        By default, the probability two random records match is estimated for the
        blocked data, and then the m and u parameters for the columns specified in the
        blocking rules are used to estiamte the global probability two random records
        match.

        To control which comparisons should have their parameter estimated, and the
        process of 'reversing out' the global probability two random records match, the
        user may specify `comparisons_to_deactivate` and
        `comparison_levels_to_reverse_blocking_rule`.   This is useful, for example
        if you block on the dmetaphone of a column but match on the original column.

        Examples:
            Default behaviour
            ```py
            br_training = "l.first_name = r.first_name and l.dob = r.dob"
            linker.estimate_parameters_using_expectation_maximisation(br_training)
            ```
            Specify which comparisons to deactivate
            ```py
            br_training = "l.dmeta_first_name = r.dmeta_first_name"
            settings_obj = linker._settings_obj
            comp = settings_obj._get_comparison_by_output_column_name("first_name")
            dmeta_level = comp._get_comparison_level_by_comparison_vector_value(1)
            linker.estimate_parameters_using_expectation_maximisation(
                br_training,
                comparisons_to_deactivate=["first_name"],
                comparison_levels_to_reverse_blocking_rule=[dmeta_level],
            )
            ```

        Args:
            blocking_rule (BlockingRuleCreator | str): The blocking rule used to
                generate pairwise record comparisons.
            comparisons_to_deactivate (list, optional): By default, splink will
                analyse the blocking rule provided and estimate the m parameters for
                all comaprisons except those included in the blocking rule.  If
                comparisons_to_deactivate are provided, spink will instead
                estimate m parameters for all comparison except those specified
                in the comparisons_to_deactivate list.  This list can either contain
                the output_column_name of the Comparison as a string, or Comparison
                objects.  Defaults to None.
            comparison_levels_to_reverse_blocking_rule (list, optional): By default,
                splink will analyse the blocking rule provided and adjust the
                global probability two random records match to account for the matches
                specified in the blocking rule. If provided, this argument will overrule
                this default behaviour. The user must provide a list of ComparisonLevel
                objects.  Defaults to None.
            estimate_without_term_frequencies (bool, optional): If True, the iterations
                of the EM algorithm ignore any term frequency adjustments and only
                depend on the comparison vectors. This allows the EM algorithm to run
                much faster, but the estimation of the parameters will change slightly.
            fix_probability_two_random_records_match (bool, optional): If True, do not
                update the probability two random records match after each iteration.
                Defaults to False.
            fix_m_probabilities (bool, optional): If True, do not update the m
                probabilities after each iteration. Defaults to False.
            fix_u_probabilities (bool, optional): If True, do not update the u
                probabilities after each iteration. Defaults to True.
            populate_probability_two_random_records_match_from_trained_values
                (bool, optional): If True, derive this parameter from
                the blocked value. Defaults to False.

        Examples:
            ```py
            blocking_rule = "l.first_name = r.first_name and l.dob = r.dob"
            linker.estimate_parameters_using_expectation_maximisation(blocking_rule)
            ```
            or using pre-built rules
            ```py
            from splink.duckdb.blocking_rule_library import block_on
            blocking_rule = block_on(["first_name", "surname"])
            linker.estimate_parameters_using_expectation_maximisation(blocking_rule)
            ```

        Returns:
            EMTrainingSession:  An object containing information about the training
                session such as how parameters changed during the iteration history

        """
        # Ensure this has been run on the main linker so that it's in the cache
        # to be used by the training linkers
        pipeline = CTEPipeline()
        compute_df_concat_with_tf(self, pipeline)

        blocking_rule_obj = to_blocking_rule_creator(blocking_rule).get_blocking_rule(
            self._sql_dialect
        )

        if type(blocking_rule_obj) not in (BlockingRule, SaltedBlockingRule):
            # TODO: seems a mismatch between message and type re: SaltedBlockingRule
            raise TypeError(
                "EM blocking rules must be plain blocking rules, not "
                "salted or exploding blocking rules"
            )

        if comparisons_to_deactivate:
            # If user provided a string, convert to Comparison object
            comparisons_to_deactivate = [
                (
                    self._settings_obj._get_comparison_by_output_column_name(n)
                    if isinstance(n, str)
                    else n
                )
                for n in comparisons_to_deactivate
            ]
            if comparison_levels_to_reverse_blocking_rule is None:
                logger.warning(
                    "\nWARNING: \n"
                    "You have provided comparisons_to_deactivate but not "
                    "comparison_levels_to_reverse_blocking_rule.\n"
                    "If comparisons_to_deactivate is provided, then "
                    "you usually need to provide corresponding "
                    "comparison_levels_to_reverse_blocking_rule "
                    "because each comparison to deactivate is effectively treated "
                    "as an exact match."
                )

        em_training_session = EMTrainingSession(
            self,
            db_api=self.db_api,
            blocking_rule_for_training=blocking_rule_obj,
            core_model_settings=self._settings_obj.core_model_settings,
            training_settings=self._settings_obj.training_settings,
            unique_id_input_columns=self._settings_obj.column_info_settings.unique_id_input_columns,
            fix_u_probabilities=fix_u_probabilities,
            fix_m_probabilities=fix_m_probabilities,
            fix_probability_two_random_records_match=fix_probability_two_random_records_match,  # noqa 501
            comparisons_to_deactivate=comparisons_to_deactivate,
            comparison_levels_to_reverse_blocking_rule=comparison_levels_to_reverse_blocking_rule,  # noqa 501
            estimate_without_term_frequencies=estimate_without_term_frequencies,
        )

        core_model_settings = em_training_session._train()
        # overwrite with the newly trained values in our linker settings
        self._settings_obj.core_model_settings = core_model_settings
        self._em_training_sessions.append(em_training_session)

        self._populate_m_u_from_trained_values()

        if populate_probability_two_random_records_match_from_trained_values:
            self._populate_probability_two_random_records_match_from_trained_values()

        self._settings_obj._columns_without_estimated_parameters_message()

        return em_training_session

    def predict(
        self,
        threshold_match_probability: float = None,
        threshold_match_weight: float = None,
        materialise_after_computing_term_frequencies: bool = True,
    ) -> SplinkDataFrame:
        """Create a dataframe of scored pairwise comparisons using the parameters
        of the linkage model.

        Uses the blocking rules specified in the
        `blocking_rules_to_generate_predictions` of the settings dictionary to
        generate the pairwise comparisons.

        Args:
            threshold_match_probability (float, optional): If specified,
                filter the results to include only pairwise comparisons with a
                match_probability above this threshold. Defaults to None.
            threshold_match_weight (float, optional): If specified,
                filter the results to include only pairwise comparisons with a
                match_weight above this threshold. Defaults to None.
            materialise_after_computing_term_frequencies (bool): If true, Splink
                will materialise the table containing the input nodes (rows)
                joined to any term frequencies which have been asked
                for in the settings object.  If False, this will be
                computed as part of one possibly gigantic CTE
                pipeline.   Defaults to True

        Examples:
            ```py
            linker = DuckDBLinker(df)
            linker.load_settings("saved_settings.json")
            df = linker.predict(threshold_match_probability=0.95)
            df.as_pandas_dataframe(limit=5)
            ```
        Returns:
            SplinkDataFrame: A SplinkDataFrame of the pairwise comparisons.  This
                represents a table materialised in the database. Methods on the
                SplinkDataFrame allow you to access the underlying data.

        """

        pipeline = CTEPipeline()

        # If materialise_after_computing_term_frequencies=False and the user only
        # calls predict, it runs as a single pipeline with no materialisation
        # of anything.

        # In duckdb, calls to random() in a CTE pipeline cause problems:
        # https://gist.github.com/RobinL/d329e7004998503ce91b68479aa41139
        if (
            materialise_after_computing_term_frequencies
            or self._sql_dialect == "duckdb"
        ):
            df_concat_with_tf = compute_df_concat_with_tf(self, pipeline)
            pipeline = CTEPipeline([df_concat_with_tf])
        else:
            pipeline = enqueue_df_concat_with_tf(self, pipeline)

        blocking_input_tablename_l = "__splink__df_concat_with_tf"
        blocking_input_tablename_r = "__splink__df_concat_with_tf"

        link_type = self._settings_obj._link_type
        if (
            len(self._input_tables_dict) == 2
            and self._settings_obj._link_type == "link_only"
        ):
            sqls = split_df_concat_with_tf_into_two_tables_sqls(
                "__splink__df_concat_with_tf",
                self._settings_obj.column_info_settings.source_dataset_column_name,
            )
            pipeline.enqueue_list_of_sqls(sqls)

            blocking_input_tablename_l = "__splink__df_concat_with_tf_left"
            blocking_input_tablename_r = "__splink__df_concat_with_tf_right"
            link_type = "two_dataset_link_only"

        # If exploded blocking rules exist, we need to materialise
        # the tables of ID pairs

        exploding_br_with_id_tables = materialise_exploded_id_tables(
            link_type=link_type,
            blocking_rules=self._settings_obj._blocking_rules_to_generate_predictions,
            db_api=self.db_api,
            splink_df_dict=self._input_tables_dict,
            source_dataset_input_column=self._settings_obj.column_info_settings.source_dataset_input_column,
            unique_id_input_column=self._settings_obj.column_info_settings.unique_id_input_column,
        )

        columns_to_select = self._settings_obj._columns_to_select_for_blocking
        sql_select_expr = ", ".join(columns_to_select)

        sqls = block_using_rules_sqls(
            input_tablename_l=blocking_input_tablename_l,
            input_tablename_r=blocking_input_tablename_r,
            blocking_rules=self._settings_obj._blocking_rules_to_generate_predictions,
            link_type=link_type,
            columns_to_select_sql=sql_select_expr,
            source_dataset_input_column=self._settings_obj.column_info_settings.source_dataset_input_column,
            unique_id_input_column=self._settings_obj.column_info_settings.unique_id_input_column,
        )

        pipeline.enqueue_list_of_sqls(sqls)

        repartition_after_blocking = getattr(self, "repartition_after_blocking", False)

        # repartition after blocking only exists on the SparkLinker
        if repartition_after_blocking:
            pipeline = pipeline.break_lineage(self.db_api)

        sql = compute_comparison_vector_values_sql(
            self._settings_obj._columns_to_select_for_comparison_vector_values
        )
        pipeline.enqueue_sql(sql, "__splink__df_comparison_vectors")

        sqls = predict_from_comparison_vectors_sqls_using_settings(
            self._settings_obj,
            threshold_match_probability,
            threshold_match_weight,
            sql_infinity_expression=self._infinity_expression,
        )
        pipeline.enqueue_list_of_sqls(sqls)

        predictions = self.db_api.sql_pipeline_to_splink_dataframe(pipeline)
        self._predict_warning()

        [b.drop_materialised_id_pairs_dataframe() for b in exploding_br_with_id_tables]

        return predictions

    def find_matches_to_new_records(
        self,
        records_or_tablename: AcceptableInputTableType | str,
        blocking_rules: list[BlockingRuleCreator | dict[str, Any] | str]
        | BlockingRuleCreator
        | dict[str, Any]
        | str = [],
        match_weight_threshold: float = -4,
    ) -> SplinkDataFrame:
        """Given one or more records, find records in the input dataset(s) which match
        and return in order of the Splink prediction score.

        This effectively provides a way of searching the input datasets
        for given record(s)

        Args:
            records_or_tablename (List[dict]): Input search record(s) as list of dict,
                or a table registered to the database.
            blocking_rules (list, optional): Blocking rules to select
                which records to find and score. If [], do not use a blocking
                rule - meaning the input records will be compared to all records
                provided to the linker when it was instantiated. Defaults to [].
            match_weight_threshold (int, optional): Return matches with a match weight
                above this threshold. Defaults to -4.

        Examples:
            ```py
            linker = DuckDBLinker(df)
            linker.load_settings("saved_settings.json")
            # Pre-compute tf tables for any tables with
            # term frequency adjustments
            linker.compute_tf_table("first_name")
            record = {'unique_id': 1,
                'first_name': "John",
                'surname': "Smith",
                'dob': "1971-05-24",
                'city': "London",
                'email': "john@smith.net"
                }
            df = linker.find_matches_to_new_records([record], blocking_rules=[])
            ```

        Returns:
            SplinkDataFrame: The pairwise comparisons.
        """

        original_blocking_rules = (
            self._settings_obj._blocking_rules_to_generate_predictions
        )
        original_link_type = self._settings_obj._link_type

        blocking_rule_list = ensure_is_list(blocking_rules)

        if not isinstance(records_or_tablename, str):
            uid = ascii_uid(8)
            new_records_tablename = f"__splink__df_new_records_{uid}"
            self.register_table(
                records_or_tablename, new_records_tablename, overwrite=True
            )

        else:
            new_records_tablename = records_or_tablename

        new_records_df = self.db_api.table_to_splink_dataframe(
            "__splink__df_new_records", new_records_tablename
        )

        pipeline = CTEPipeline()
        nodes_with_tf = compute_df_concat_with_tf(self, pipeline)

        pipeline = CTEPipeline([nodes_with_tf, new_records_df])
        if len(blocking_rule_list) == 0:
            blocking_rule_list = [BlockingRule("1=1")]
        blocking_rule_list = [blocking_rule_to_obj(br) for br in blocking_rule_list]
        for n, br in enumerate(blocking_rule_list):
            br.add_preceding_rules(blocking_rule_list[:n])

        self._settings_obj._blocking_rules_to_generate_predictions = blocking_rule_list

        for tf_col in self._settings_obj._term_frequency_columns:
            tf_table_name = colname_to_tf_tablename(tf_col)
            if tf_table_name in self._intermediate_table_cache:
                tf_table = self._intermediate_table_cache.get_with_logging(
                    tf_table_name
                )
                pipeline.append_input_dataframe(tf_table)

        sql = _join_new_table_to_df_concat_with_tf_sql(self, "__splink__df_new_records")
        pipeline.enqueue_sql(sql, "__splink__df_new_records_with_tf_before_uid_fix")

        pipeline = add_unique_id_and_source_dataset_cols_if_needed(
            self, new_records_df, pipeline
        )
        settings = self._settings_obj
        sqls = block_using_rules_sqls(
            input_tablename_l="__splink__df_concat_with_tf",
            input_tablename_r="__splink__df_new_records_with_tf",
            blocking_rules=blocking_rule_list,
            link_type="two_dataset_link_only",
            columns_to_select_sql=", ".join(settings._columns_to_select_for_blocking),
            source_dataset_input_column=settings.column_info_settings.source_dataset_input_column,
            unique_id_input_column=settings.column_info_settings.unique_id_input_column,
        )
        pipeline.enqueue_list_of_sqls(sqls)

        sql = compute_comparison_vector_values_sql(
            self._settings_obj._columns_to_select_for_comparison_vector_values
        )
        pipeline.enqueue_sql(sql, "__splink__df_comparison_vectors")

        sqls = predict_from_comparison_vectors_sqls_using_settings(
            self._settings_obj,
            sql_infinity_expression=self._infinity_expression,
        )
        pipeline.enqueue_list_of_sqls(sqls)

        sql = f"""
        select * from __splink__df_predict
        where match_weight > {match_weight_threshold}
        """

        pipeline.enqueue_sql(sql, "__splink__find_matches_predictions")

        predictions = self.db_api.sql_pipeline_to_splink_dataframe(
            pipeline, use_cache=False
        )

        self._settings_obj._blocking_rules_to_generate_predictions = (
            original_blocking_rules
        )
        self._settings_obj._link_type = original_link_type

        return predictions

    def compare_two_records(
        self, record_1: dict[str, Any], record_2: dict[str, Any]
    ) -> SplinkDataFrame:
        """Use the linkage model to compare and score a pairwise record comparison
        based on the two input records provided

        Args:
            record_1 (dict): dictionary representing the first record.  Columns names
                and data types must be the same as the columns in the settings object
            record_2 (dict): dictionary representing the second record.  Columns names
                and data types must be the same as the columns in the settings object

        Examples:
            ```py
            linker = DuckDBLinker(df)
            linker.load_settings("saved_settings.json")
            linker.compare_two_records(record_left, record_right)
            ```

        Returns:
            SplinkDataFrame: Pairwise comparison with scored prediction
        """

        cache = self._intermediate_table_cache

        uid = ascii_uid(8)
        df_records_left = self.register_table(
            [record_1], f"__splink__compare_two_records_left_{uid}", overwrite=True
        )
        df_records_left.templated_name = "__splink__compare_two_records_left"

        df_records_right = self.register_table(
            [record_2], f"__splink__compare_two_records_right_{uid}", overwrite=True
        )
        df_records_right.templated_name = "__splink__compare_two_records_right"

        pipeline = CTEPipeline([df_records_left, df_records_right])

        if "__splink__df_concat_with_tf" in cache:
            nodes_with_tf = cache.get_with_logging("__splink__df_concat_with_tf")
            pipeline.append_input_dataframe(nodes_with_tf)

        for tf_col in self._settings_obj._term_frequency_columns:
            tf_table_name = colname_to_tf_tablename(tf_col)
            if tf_table_name in cache:
                tf_table = cache.get_with_logging(tf_table_name)
                pipeline.append_input_dataframe(tf_table)
            else:
                if "__splink__df_concat_with_tf" not in cache:
                    logger.warning(
                        f"No term frequencies found for column {tf_col.name}.\n"
                        "To apply term frequency adjustments, you need to register"
                        " a lookup using `linker.register_term_frequency_lookup`."
                    )

        sql_join_tf = _join_new_table_to_df_concat_with_tf_sql(
            self, "__splink__compare_two_records_left"
        )

        pipeline.enqueue_sql(sql_join_tf, "__splink__compare_two_records_left_with_tf")

        sql_join_tf = _join_new_table_to_df_concat_with_tf_sql(
            self, "__splink__compare_two_records_right"
        )

        pipeline.enqueue_sql(sql_join_tf, "__splink__compare_two_records_right_with_tf")

        sqls = block_using_rules_sqls(
            input_tablename_l="__splink__compare_two_records_left_with_tf",
            input_tablename_r="__splink__compare_two_records_right_with_tf",
            blocking_rules=[BlockingRule("1=1")],
            link_type=self._settings_obj._link_type,
            columns_to_select_sql=", ".join(
                self._settings_obj._columns_to_select_for_blocking
            ),
            source_dataset_input_column=self._settings_obj.column_info_settings.source_dataset_input_column,
            unique_id_input_column=self._settings_obj.column_info_settings.unique_id_input_column,
        )
        pipeline.enqueue_list_of_sqls(sqls)

        sql = compute_comparison_vector_values_sql(
            self._settings_obj._columns_to_select_for_comparison_vector_values
        )
        pipeline.enqueue_sql(sql, "__splink__df_comparison_vectors")

        sqls = predict_from_comparison_vectors_sqls_using_settings(
            self._settings_obj,
            sql_infinity_expression=self._infinity_expression,
        )
        pipeline.enqueue_list_of_sqls(sqls)

        predictions = self.db_api.sql_pipeline_to_splink_dataframe(
            pipeline, use_cache=False
        )

        return predictions

    def _self_link(self) -> SplinkDataFrame:
        """Use the linkage model to compare and score all records in our input df with
            themselves.

        Returns:
            SplinkDataFrame: Scored pairwise comparisons of the input records to
                themselves.
        """

        # Block on uid i.e. create pairwise record comparisons where the uid matches
        settings = self._settings_obj
        uid_cols = settings.column_info_settings.unique_id_input_columns
        uid_l = _composite_unique_id_from_edges_sql(uid_cols, None, "l")
        uid_r = _composite_unique_id_from_edges_sql(uid_cols, None, "r")

        blocking_rule = BlockingRule(
            f"{uid_l} = {uid_r}", sqlglot_dialect=self._sql_dialect
        )

        pipeline = CTEPipeline()
        nodes_with_tf = compute_df_concat_with_tf(self, pipeline)

        pipeline = CTEPipeline([nodes_with_tf])

        sqls = block_using_rules_sqls(
            input_tablename_l="__splink__df_concat_with_tf",
            input_tablename_r="__splink__df_concat_with_tf",
            blocking_rules=[blocking_rule],
            link_type="self_link",
            columns_to_select_sql=", ".join(settings._columns_to_select_for_blocking),
            source_dataset_input_column=settings.column_info_settings.source_dataset_input_column,
            unique_id_input_column=settings.column_info_settings.unique_id_input_column,
        )
        pipeline.enqueue_list_of_sqls(sqls)

        sql = compute_comparison_vector_values_sql(
            self._settings_obj._columns_to_select_for_comparison_vector_values
        )

        pipeline.enqueue_sql(sql, "__splink__df_comparison_vectors")

        sql_infos = predict_from_comparison_vectors_sqls(
            unique_id_input_columns=uid_cols,
            core_model_settings=self._settings_obj.core_model_settings,
            sql_dialect=self._sql_dialect,
            sql_infinity_expression=self._infinity_expression,
        )
        for sql_info in sql_infos:
            output_table_name = sql_info["output_table_name"]
            output_table_name = output_table_name.replace("predict", "self_link")
            pipeline.enqueue_sql(sql_info["sql"], output_table_name)

        predictions = self.db_api.sql_pipeline_to_splink_dataframe(pipeline)

        return predictions

    def cluster_pairwise_predictions_at_threshold(
        self,
        df_predict: SplinkDataFrame,
        threshold_match_probability: Optional[float] = None,
        pairwise_formatting: bool = False,
        filter_pairwise_format_for_clusters: bool = True,
    ) -> SplinkDataFrame:
        """Clusters the pairwise match predictions that result from `linker.predict()`
        into groups of connected record using the connected components graph clustering
        algorithm

        Records with an estimated `match_probability` at or above
        `threshold_match_probability` are considered to be a match (i.e. they represent
        the same entity).

        Args:
            df_predict (SplinkDataFrame): The results of `linker.predict()`
            threshold_match_probability (float): Filter the pairwise match predictions
                to include only pairwise comparisons with a match_probability at or
                above this threshold. This dataframe is then fed into the clustering
                algorithm.
            pairwise_formatting (bool): Whether to output the pairwise match predictions
                from linker.predict() with cluster IDs.
                If this is set to false, the output will be a list of all IDs, clustered
                into groups based on the desired match threshold.
            filter_pairwise_format_for_clusters (bool): If pairwise formatting has been
                selected, whether to output all columns found within linker.predict(),
                or just return clusters.

        Returns:
            SplinkDataFrame: A SplinkDataFrame containing a list of all IDs, clustered
                into groups based on the desired match threshold.

        """

        # Feeding in df_predict forces materiailisation, if it exists in your database
        pipeline = CTEPipeline()
        nodes_with_tf = compute_df_concat_with_tf(self, pipeline)

        edges_table = _cc_create_unique_id_cols(
            self,
            nodes_with_tf.physical_name,
            df_predict,
            threshold_match_probability,
        )

        cc = solve_connected_components(
            self,
            edges_table,
            df_predict,
            nodes_with_tf,
            pairwise_formatting,
            filter_pairwise_format_for_clusters,
        )
        cc.metadata["threshold_match_probability"] = threshold_match_probability

        return cc

    def _compute_metrics_nodes(
        self,
        df_predict: SplinkDataFrame,
        df_clustered: SplinkDataFrame,
        threshold_match_probability: float,
    ) -> SplinkDataFrame:
        """
        Internal function for computing node-level metrics.

        Accepts outputs of `linker.predict()` and
        `linker.cluster_pairwise_at_threshold()`, along with the clustering threshold
        and produces a table of node metrics.

        Node metrics produced:
        * node_degree (absolute number of neighbouring nodes)

        Output table has a single row per input node, along with the cluster id (as
        assigned in `linker.cluster_pairwise_at_threshold()`) and the metric
        node_degree:
        |-------------------------------------------------|
        | composite_unique_id | cluster_id  | node_degree |
        |---------------------|-------------|-------------|
        | s1-__-10001         | s1-__-10001 | 6           |
        | s1-__-10002         | s1-__-10001 | 4           |
        | s1-__-10003         | s1-__-10003 | 2           |
        ...
        """
        uid_cols = self._settings_obj.column_info_settings.unique_id_input_columns
        # need composite unique ids
        composite_uid_edges_l = _composite_unique_id_from_edges_sql(uid_cols, "l")
        composite_uid_edges_r = _composite_unique_id_from_edges_sql(uid_cols, "r")
        composite_uid_clusters = _composite_unique_id_from_nodes_sql(uid_cols)

        pipeline = CTEPipeline()
        sqls = _node_degree_sql(
            df_predict,
            df_clustered,
            composite_uid_edges_l,
            composite_uid_edges_r,
            composite_uid_clusters,
            threshold_match_probability,
        )
        pipeline.enqueue_list_of_sqls(sqls)

        df_node_metrics = self.db_api.sql_pipeline_to_splink_dataframe(pipeline)

        df_node_metrics.metadata["threshold_match_probability"] = (
            threshold_match_probability
        )
        return df_node_metrics

    def _compute_metrics_edges(
        self,
        df_node_metrics: SplinkDataFrame,
        df_predict: SplinkDataFrame,
        df_clustered: SplinkDataFrame,
        threshold_match_probability: float,
    ) -> SplinkDataFrame:
        """
        Internal function for computing edge-level metrics.

        Accepts outputs of `linker._compute_node_metrics()`, `linker.predict()` and
        `linker.cluster_pairwise_at_threshold()`, along with the clustering threshold
        and produces a table of edge metrics.

        Uses `igraph` under-the-hood for calculations

        Edge metrics produced:
        * is_bridge (is the edge a bridge?)

        Output table has a single row per edge, and the metric is_bridge:
        |-------------------------------------------------------------|
        | composite_unique_id_l | composite_unique_id_r   | is_bridge |
        |-----------------------|-------------------------|-----------|
        | s1-__-10001           | s1-__-10003             | True      |
        | s1-__-10001           | s1-__-10005             | False     |
        | s1-__-10005           | s1-__-10009             | False     |
        | s1-__-10021           | s1-__-10024             | True      |
        ...
        """
        df_edge_metrics = compute_edge_metrics(
            self, df_node_metrics, df_predict, df_clustered, threshold_match_probability
        )
        df_edge_metrics.metadata["threshold_match_probability"] = (
            threshold_match_probability
        )
        return df_edge_metrics

    def _compute_metrics_clusters(
        self,
        df_node_metrics: SplinkDataFrame,
    ) -> SplinkDataFrame:
        """
        Internal function for computing cluster-level metrics.

        Accepts output of `linker._compute_node_metrics()` (which has the relevant
        information from `linker.predict() and
        `linker.cluster_pairwise_at_threshold()`), produces a table of cluster metrics.

        Cluster metrics produced:
        * n_nodes (aka cluster size, number of nodes in cluster)
        * n_edges (number of edges in cluster)
        * density (number of edges normalised wrt maximum possible number)
        * cluster_centralisation (average absolute deviation from maximum node_degree
            normalised wrt maximum possible value)

        Output table has a single row per cluster, along with the cluster metrics
        listed above
        |--------------------------------------------------------------------|
        | cluster_id  | n_nodes | n_edges | density | cluster_centralisation |
        |-------------|---------|---------|---------|------------------------|
        | s1-__-10006 | 4       | 4       | 0.66667 | 0.6666                 |
        | s1-__-10008 | 6       | 5       | 0.33333 | 0.4                    |
        | s1-__-10013 | 11      | 19      | 0.34545 | 0.3111                 |
        ...
        """
        pipeline = CTEPipeline()
        sqls = _size_density_centralisation_sql(
            df_node_metrics,
        )
        pipeline.enqueue_list_of_sqls(sqls)

        df_cluster_metrics = self.db_api.sql_pipeline_to_splink_dataframe(pipeline)
        df_cluster_metrics.metadata["threshold_match_probability"] = (
            df_node_metrics.metadata["threshold_match_probability"]
        )
        return df_cluster_metrics

    def compute_graph_metrics(
        self,
        df_predict: SplinkDataFrame,
        df_clustered: SplinkDataFrame,
        *,
        threshold_match_probability: float = None,
    ) -> GraphMetricsResults:
        """
        Generates tables containing graph metrics (for nodes, edges and clusters),
        and returns a data class of Splink dataframes

        Args:
            df_predict (SplinkDataFrame): The results of `linker.predict()`
            df_clustered (SplinkDataFrame): The outputs of
                `linker.cluster_pairwise_predictions_at_threshold()`
            threshold_match_probability (float, optional): Filter the pairwise match
                predictions to include only pairwise comparisons with a
                match_probability at or above this threshold. If not provided, the value
                will be taken from metadata on `df_clustered`. If no such metadata is
                available, this value _must_ be provided.

        Returns:
            GraphMetricsResult: A data class containing SplinkDataFrames
            of cluster IDs and selected node, edge or cluster metrics.
                attribute "nodes" for nodes metrics table
                attribute "edges" for edge metrics table
                attribute "clusters" for cluster metrics table

        """
        if threshold_match_probability is None:
            threshold_match_probability = df_clustered.metadata.get(
                "threshold_match_probability", None
            )
            # we may not have metadata if clusters have been manually registered, or
            # read in from a format that does not include it
            if threshold_match_probability is None:
                raise TypeError(
                    "As `df_clustered` has no threshold metadata associated to it, "
                    "to compute graph metrics you must provide "
                    "`threshold_match_probability` manually"
                )
        df_node_metrics = self._compute_metrics_nodes(
            df_predict, df_clustered, threshold_match_probability
        )
        df_edge_metrics = self._compute_metrics_edges(
            df_node_metrics,
            df_predict,
            df_clustered,
            threshold_match_probability,
        )
        # don't need edges as information is baked into node metrics
        df_cluster_metrics = self._compute_metrics_clusters(df_node_metrics)

        return GraphMetricsResults(
            nodes=df_node_metrics, edges=df_edge_metrics, clusters=df_cluster_metrics
        )

    def _get_labels_tablename_from_input(
        self, labels_splinkdataframe_or_table_name: str | SplinkDataFrame
    ) -> str:
        if isinstance(labels_splinkdataframe_or_table_name, SplinkDataFrame):
            labels_tablename = labels_splinkdataframe_or_table_name.physical_name
        elif isinstance(labels_splinkdataframe_or_table_name, str):
            labels_tablename = labels_splinkdataframe_or_table_name
        else:
            raise ValueError(
                "The 'labels_splinkdataframe_or_table_name' argument"
                " must be of type SplinkDataframe or a string representing a tablename"
                " in the input database"
            )
        return labels_tablename

    def estimate_m_from_pairwise_labels(self, labels_splinkdataframe_or_table_name):
        """Estimate the m parameters of the linkage model from a dataframe of pairwise
        labels.

        The table of labels should be in the following format, and should
        be registered with your database:
        |source_dataset_l|unique_id_l|source_dataset_r|unique_id_r|
        |----------------|-----------|----------------|-----------|
        |df_1            |1          |df_2            |2          |
        |df_1            |1          |df_2            |3          |

        Note that `source_dataset` and `unique_id` should correspond to the
        values specified in the settings dict, and the `input_table_aliases`
        passed to the `linker` object. Note that at the moment, this method does
        not respect values in a `clerical_match_score` column.  If provided, these
        are ignored and it is assumed that every row in the table of labels is a score
        of 1, i.e. a perfect match.

        Args:
          labels_splinkdataframe_or_table_name (str): Name of table containing labels
            in the database or SplinkDataframe

        Examples:
            ```py
            pairwise_labels = pd.read_csv("./data/pairwise_labels_to_estimate_m.csv")
            linker.register_table(pairwise_labels, "labels", overwrite=True)
            linker.estimate_m_from_pairwise_labels("labels")
            ```
        """
        labels_tablename = self._get_labels_tablename_from_input(
            labels_splinkdataframe_or_table_name
        )
        estimate_m_from_pairwise_labels(self, labels_tablename)

    def truth_space_table_from_labels_table(
        self,
        labels_splinkdataframe_or_table_name: SplinkDataFrame | str,
        threshold_actual: float = 0.5,
        match_weight_round_to_nearest: float = None,
    ) -> SplinkDataFrame:
        """Generate truth statistics (false positive etc.) for each threshold value of
        match_probability, suitable for plotting a ROC chart.

        The table of labels should be in the following format, and should be registered
        with your database:

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
                the number of points plotted on the ROC chart. Defaults to None.

        Examples:
            ```py
            labels = pd.read_csv("my_labels.csv")
            linker.register_table(labels, "labels")
            linker.truth_space_table_from_labels_table("labels")
            ```

        Returns:
            SplinkDataFrame:  Table of truth statistics
        """
        labels_tablename = self._get_labels_tablename_from_input(
            labels_splinkdataframe_or_table_name
        )

        self._raise_error_if_necessary_accuracy_columns_not_computed()
        return truth_space_table_from_labels_table(
            self,
            labels_tablename,
            threshold_actual=threshold_actual,
            match_weight_round_to_nearest=match_weight_round_to_nearest,
        )

    def roc_chart_from_labels_table(
        self,
        labels_splinkdataframe_or_table_name: str | SplinkDataFrame,
        threshold_actual: float = 0.5,
        match_weight_round_to_nearest: float = None,
    ) -> ChartReturnType:
        """Generate a ROC chart from labelled (ground truth) data.

        The table of labels should be in the following format, and should be registered
        with your database:

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
                the number of points plotted on the ROC chart. Defaults to None.

        Examples:
            === ":simple-duckdb: DuckDB"
                ```py
                labels = pd.read_csv("my_labels.csv")
                linker.register_table(labels, "labels")
                linker.roc_chart_from_labels_table("labels")
                ```
            === ":simple-apachespark: Spark"
                ```py
                labels = spark.read.csv("my_labels.csv", header=True)
                labels.createDataFrame("labels")
                linker.roc_chart_from_labels_table("labels")
                ```

        Returns:
            altair.Chart: An altair chart
        """
        labels_tablename = self._get_labels_tablename_from_input(
            labels_splinkdataframe_or_table_name
        )

        self._raise_error_if_necessary_accuracy_columns_not_computed()
        df_truth_space = truth_space_table_from_labels_table(
            self,
            labels_tablename,
            threshold_actual=threshold_actual,
            match_weight_round_to_nearest=match_weight_round_to_nearest,
        )
        recs = df_truth_space.as_record_dict()
        return roc_chart(recs)

    def precision_recall_chart_from_labels_table(
        self,
        labels_splinkdataframe_or_table_name: SplinkDataFrame | str,
        threshold_actual: float = 0.5,
        match_weight_round_to_nearest: float = None,
    ) -> ChartReturnType:
        """Generate a precision-recall chart from labelled (ground truth) data.

        The table of labels should be in the following format, and should be registered
        as a table with your database:

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
                the number of points plotted on the ROC chart. Defaults to None.
        Examples:
            ```py
            labels = pd.read_csv("my_labels.csv")
            linker.register_table(labels, "labels")
            linker.precision_recall_chart_from_labels_table("labels")
            ```

        Returns:
            altair.Chart: An altair chart
        """
        labels_tablename = self._get_labels_tablename_from_input(
            labels_splinkdataframe_or_table_name
        )
        self._raise_error_if_necessary_accuracy_columns_not_computed()
        df_truth_space = truth_space_table_from_labels_table(
            self,
            labels_tablename,
            threshold_actual=threshold_actual,
            match_weight_round_to_nearest=match_weight_round_to_nearest,
        )
        recs = df_truth_space.as_record_dict()
        return precision_recall_chart(recs)

    def accuracy_chart_from_labels_table(
        self,
        labels_splinkdataframe_or_table_name: SplinkDataFrame | str,
        threshold_actual: float = 0.5,
        match_weight_round_to_nearest: float = None,
        add_metrics: list[str] = [],
    ) -> ChartReturnType:
        """Generate an accuracy measure chart from labelled (ground truth) data.

        The table of labels should be in the following format, and should be registered
        as a table with your database:

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
            === ":simple-duckdb: DuckDB"
                ```py
                labels = pd.read_csv("my_labels.csv")
                linker.register_table(labels, "labels")
                linker.accuracy_chart_from_labels_table("labels", add_metrics=["f1"])
                ```
            === ":simple-apachespark: Spark"
                ```py
                labels = spark.read.csv("my_labels.csv", header=True)
                labels.createDataFrame("labels")
                linker.accuracy_chart_from_labels_table("labels", add_metrics=['f1'])
                ```

        Returns:
            altair.Chart: An altair chart
        """
        allowed = ["specificity", "npv", "accuracy", "f1", "f2", "f0_5", "p4", "phi"]

        if not isinstance(add_metrics, list):
            raise Exception(
                "add_metrics must be a list containing one or more of the following:",
                allowed,
            )

        # Silently filter out invalid entries (except case errors - e.g. ["NPV", "F1"])
        add_metrics = list(set(map(str.lower, add_metrics)).intersection(allowed))

        labels_tablename = self._get_labels_tablename_from_input(
            labels_splinkdataframe_or_table_name
        )
        self._raise_error_if_necessary_accuracy_columns_not_computed()
        df_truth_space = truth_space_table_from_labels_table(
            self,
            labels_tablename,
            threshold_actual=threshold_actual,
            match_weight_round_to_nearest=match_weight_round_to_nearest,
        )
        recs = df_truth_space.as_record_dict()
        return accuracy_chart(recs, add_metrics=add_metrics)

    def threshold_selection_tool_from_labels_table(
        self,
        labels_splinkdataframe_or_table_name: str | SplinkDataFrame,
        threshold_actual: float = 0.5,
        match_weight_round_to_nearest: float = None,
        add_metrics: list[str] = [],
    ) -> ChartReturnType:
        """Generate an accuracy chart from labelled (ground truth) data.

        The table of labels should be in the following format, and should be registered
        as a table with your database:

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
            linker.accuracy_chart_from_labels_column("ground_truth", add_metrics=["f1"])
            ```

        Returns:
            altair.Chart: An altair chart
        """

        allowed = ["specificity", "npv", "accuracy", "f1", "f2", "f0_5", "p4", "phi"]

        if not isinstance(add_metrics, list):
            raise Exception(
                "add_metrics must be a list containing one or more of the following:",
                allowed,
            )

        # Silently filter out invalid entries (except case errors - e.g. ["NPV", "F1"])
        add_metrics = list(set(map(str.lower, add_metrics)).intersection(allowed))

        labels_tablename = self._get_labels_tablename_from_input(
            labels_splinkdataframe_or_table_name
        )
        self._raise_error_if_necessary_accuracy_columns_not_computed()
        df_truth_space = truth_space_table_from_labels_table(
            self,
            labels_tablename,
            threshold_actual=threshold_actual,
            match_weight_round_to_nearest=match_weight_round_to_nearest,
        )
        recs = df_truth_space.as_record_dict()
        return threshold_selection_tool(recs, add_metrics=add_metrics)

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

    def truth_space_table_from_labels_column(
        self,
        labels_column_name: str,
        threshold_actual: float = 0.5,
        match_weight_round_to_nearest: float = None,
        positives_not_captured_by_blocking_rules_scored_as_zero: bool = True,
    ) -> SplinkDataFrame:
        """Generate truth statistics (false positive etc.) for each threshold value of
        match_probability, suitable for plotting a ROC chart.

        Your labels_column_name should include the ground truth cluster (unique
        identifier) that groups entities which are the same

        Args:
            labels_tablename (str): Name of table containing labels in the database
            threshold_actual (float, optional): Where the `clerical_match_score`
                provided by the user is a probability rather than binary, this value
                is used as the threshold to classify `clerical_match_score`s as binary
                matches or non matches. Defaults to 0.5.
            match_weight_round_to_nearest (float, optional): When provided, thresholds
                are rounded.  When large numbers of labels are provided, this is
                sometimes necessary to reduce the size of the ROC table, and therefore
                the number of points plotted on the ROC chart. Defaults to None.

        Examples:
            ```py
            linker.truth_space_table_from_labels_column("cluster")
            ```

        Returns:
            SplinkDataFrame:  Table of truth statistics
        """

        return truth_space_table_from_labels_column(
            self,
            labels_column_name,
            threshold_actual,
            match_weight_round_to_nearest,
            positives_not_captured_by_blocking_rules_scored_as_zero,
        )

    def roc_chart_from_labels_column(
        self,
        labels_column_name: str,
        threshold_actual: float = 0.5,
        match_weight_round_to_nearest: float = None,
    ) -> ChartReturnType:
        """Generate a ROC chart from ground truth data, whereby the ground truth
        is in a column in the input dataset called `labels_column_name`

        Args:
            labels_column_name (str): Column name containing labels in the input table
            threshold_actual (float, optional): Where the `clerical_match_score`
                provided by the user is a probability rather than binary, this value
                is used as the threshold to classify `clerical_match_score`s as binary
                matches or non matches. Defaults to 0.5.
            match_weight_round_to_nearest (float, optional): When provided, thresholds
                are rounded.  When large numbers of labels are provided, this is
                sometimes necessary to reduce the size of the ROC table, and therefore
                the number of points plotted on the ROC chart. Defaults to None.

        Examples:
            ```py
            linker.roc_chart_from_labels_column("labels")
            ```

        Returns:
            altair.Chart: An altair chart
        """

        df_truth_space = truth_space_table_from_labels_column(
            self,
            labels_column_name,
            threshold_actual=threshold_actual,
            match_weight_round_to_nearest=match_weight_round_to_nearest,
        )
        recs = df_truth_space.as_record_dict()
        return roc_chart(recs)

    def precision_recall_chart_from_labels_column(
        self,
        labels_column_name: str,
        threshold_actual: float = 0.5,
        match_weight_round_to_nearest: float = None,
    ) -> ChartReturnType:
        """Generate a precision-recall chart from ground truth data, whereby the ground
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
                the number of points plotted on the ROC chart. Defaults to None.
        Examples:
            ```py
            linker.precision_recall_chart_from_labels_column("ground_truth")
            ```

        Returns:
            altair.Chart: An altair chart
        """

        df_truth_space = truth_space_table_from_labels_column(
            self,
            labels_column_name,
            threshold_actual=threshold_actual,
            match_weight_round_to_nearest=match_weight_round_to_nearest,
        )
        recs = df_truth_space.as_record_dict()
        return precision_recall_chart(recs)

    def accuracy_chart_from_labels_column(
        self,
        labels_column_name: str,
        threshold_actual: float = 0.5,
        match_weight_round_to_nearest: float = None,
        add_metrics: list[str] = [],
    ) -> ChartReturnType:
        """Generate an accuracy chart from ground truth data, whereby the ground
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
            linker.accuracy_chart_from_labels_column("ground_truth", add_metrics=["f1"])
            ```

        Returns:
            altair.Chart: An altair chart
        """

        allowed = ["specificity", "npv", "accuracy", "f1", "f2", "f0_5", "p4", "phi"]

        if not isinstance(add_metrics, list):
            raise Exception(
                "add_metrics must be a list containing one or more of the following:",
                allowed,
            )

        # Silently filter out invalid entries (except case errors - e.g. ["NPV", "F1"])
        add_metrics = list(set(map(str.lower, add_metrics)).intersection(allowed))

        df_truth_space = truth_space_table_from_labels_column(
            self,
            labels_column_name,
            threshold_actual=threshold_actual,
            match_weight_round_to_nearest=match_weight_round_to_nearest,
        )
        recs = df_truth_space.as_record_dict()
        return accuracy_chart(recs, add_metrics=add_metrics)

    def threshold_selection_tool_from_labels_column(
        self,
        labels_column_name: str,
        threshold_actual: float = 0.5,
        match_weight_round_to_nearest: float = None,
        add_metrics: list[str] = [],
    ) -> ChartReturnType:
        """Generate an accuracy chart from ground truth data, whereby the ground
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
            linker.accuracy_chart_from_labels_column("ground_truth", add_metrics=["f1"])
            ```

        Returns:
            altair.Chart: An altair chart
        """

        allowed = ["specificity", "npv", "accuracy", "f1", "f2", "f0_5", "p4", "phi"]

        if not isinstance(add_metrics, list):
            raise Exception(
                "add_metrics must be a list containing one or more of the following:",
                allowed,
            )

        # Silently filter out invalid entries (except case errors - e.g. ["NPV", "F1"])
        add_metrics = list(set(map(str.lower, add_metrics)).intersection(allowed))

        df_truth_space = truth_space_table_from_labels_column(
            self,
            labels_column_name,
            threshold_actual=threshold_actual,
            match_weight_round_to_nearest=match_weight_round_to_nearest,
        )
        recs = df_truth_space.as_record_dict()
        return threshold_selection_tool(recs, add_metrics=add_metrics)

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
            self,
            label_colname,
            include_false_positives,
            include_false_negatives,
            threshold,
        )

    def match_weights_histogram(
        self,
        df_predict: SplinkDataFrame,
        target_bins: int = 30,
        width: int = 600,
        height: int = 250,
    ) -> ChartReturnType:
        """Generate a histogram that shows the distribution of match weights in
        `df_predict`

        Args:
            df_predict (SplinkDataFrame): Output of `linker.predict()`
            target_bins (int, optional): Target number of bins in histogram. Defaults to
                30.
            width (int, optional): Width of output. Defaults to 600.
            height (int, optional): Height of output chart. Defaults to 250.


        Returns:
            altair.Chart: An altair chart

        """
        df = histogram_data(self, df_predict, target_bins)
        recs = df.as_record_dict()
        return match_weights_histogram(recs, width=width, height=height)

    def waterfall_chart(
        self,
        records: list[dict[str, Any]],
        filter_nulls: bool = True,
        remove_sensitive_data: bool = False,
    ) -> ChartReturnType:
        """Visualise how the final match weight is computed for the provided pairwise
        record comparisons.

        Records must be provided as a list of dictionaries. This would usually be
        obtained from `df.as_record_dict(limit=n)` where `df` is a SplinkDataFrame.

        Examples:
            ```py
            df = linker.predict(threshold_match_weight=2)
            records = df.as_record_dict(limit=10)
            linker.waterfall_chart(records)
            ```

        Args:
            records (List[dict]): Usually be obtained from `df.as_record_dict(limit=n)`
                where `df` is a SplinkDataFrame.
            filter_nulls (bool, optional): Whether the visualiation shows null
                comparisons, which have no effect on final match weight. Defaults to
                True.
            remove_sensitive_data (bool, optional): When True, The waterfall chart will
                contain match weights only, and all of the (potentially sensitive) data
                from the input tables will be removed prior to the chart being created.


        Returns:
            altair.Chart: An altair chart

        """
        self._raise_error_if_necessary_waterfall_columns_not_computed()

        return waterfall_chart(
            records, self._settings_obj, filter_nulls, remove_sensitive_data
        )

    def unlinkables_chart(
        self,
        x_col: str = "match_weight",
        name_of_data_in_title: str | None = None,
        as_dict: bool = False,
    ) -> ChartReturnType:
        """Generate an interactive chart displaying the proportion of records that
        are "unlinkable" for a given splink score threshold and model parameters.

        Unlinkable records are those that, even when compared with themselves, do not
        contain enough information to confirm a match.

        Args:
            x_col (str, optional): Column to use for the x-axis.
                Defaults to "match_weight".
            name_of_data_in_title (str, optional): Name of the source dataset to use for
                the title of the output chart.
            as_dict (bool, optional): If True, return a dict version of the chart.

        Examples:
            For the simplest code pipeline, load a pre-trained model
            and run this against the test data.
            ```py
            from splink.datasets import splink_datasets
            df = splink_datasets.fake_1000
            linker = DuckDBLinker(df)
            linker.load_settings("saved_settings.json")
            linker.unlinkables_chart()
            ```
            For more complex code pipelines, you can run an entire pipeline
            that estimates your m and u values, before `unlinkables_chart().

        Returns:
            altair.Chart: An altair chart
        """

        # Link our initial df on itself and calculate the % of unlinkable entries
        records = unlinkables_data(self)
        return unlinkables_chart(records, x_col, name_of_data_in_title, as_dict)

    def comparison_viewer_dashboard(
        self,
        df_predict: SplinkDataFrame,
        out_path: str,
        overwrite: bool = False,
        num_example_rows: int = 2,
        return_html_as_string: bool = False,
    ) -> str | None:
        """Generate an interactive html visualization of the linker's predictions and
        save to `out_path`.  For more information see
        [this video](https://www.youtube.com/watch?v=DNvCMqjipis)


        Args:
            df_predict (SplinkDataFrame): The outputs of `linker.predict()`
            out_path (str): The path (including filename) to save the html file to.
            overwrite (bool, optional): Overwrite the html file if it already exists?
                Defaults to False.
            num_example_rows (int, optional): Number of example rows per comparison
                vector. Defaults to 2.
            return_html_as_string: If True, return the html as a string

        Examples:
            ```py
            df_predictions = linker.predict()
            linker.comparison_viewer_dashboard(df_predictions, "scv.html", True, 2)
            ```

            Optionally, in Jupyter, you can display the results inline
            Otherwise you can just load the html file in your browser
            ```py
            from IPython.display import IFrame
            IFrame(src="./scv.html", width="100%", height=1200)
            ```

        """
        self._raise_error_if_necessary_waterfall_columns_not_computed()
        pipeline = CTEPipeline([df_predict])
        sql = comparison_vector_distribution_sql(self)
        pipeline.enqueue_sql(sql, "__splink__df_comparison_vector_distribution")

        sqls = comparison_viewer_table_sqls(self, num_example_rows)
        pipeline.enqueue_list_of_sqls(sqls)

        df = self.db_api.sql_pipeline_to_splink_dataframe(pipeline)

        rendered = render_splink_comparison_viewer_html(
            df.as_record_dict(),
            self._settings_obj._as_completed_dict(),
            out_path,
            overwrite,
        )
        if return_html_as_string:
            return rendered
        return None

    def parameter_estimate_comparisons_chart(
        self, include_m: bool = True, include_u: bool = False
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

        """
        records = self._settings_obj._parameter_estimates_as_records

        to_retain = []
        if include_m:
            to_retain.append("m")
        if include_u:
            to_retain.append("u")

        records = [r for r in records if r["m_or_u"] in to_retain]

        return parameter_estimate_comparisons(records)

    def match_weights_chart(self):
        """Display a chart of the (partial) match weights of the linkage model

        Examples:
            ```py
            linker.match_weights_chart()
            ```
            To view offline (if you don't have an internet connection):
            ```py
            from splink.charts import save_offline_chart
            c = linker.match_weights_chart()
            save_offline_chart(c.to_dict(), "test_chart.html")
            ```
            View resultant html file in Jupyter (or just load it in your browser)
            ```py
            from IPython.display import IFrame
            IFrame(src="./test_chart.html", width=1000, height=500)
            ```

        Returns:
            altair.Chart: An altair chart
        """
        return self._settings_obj.match_weights_chart()

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
                sfrequency adjustments.
                Defaults to None.

        Returns:
            altair.Chart: An altair chart
        """

        # Comparisons with TF adjustments
        tf_comparisons = [
            c.output_column_name
            for c in self._settings_obj.comparisons
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
            self,
            output_column_name,
            n_most_freq,
            n_least_freq,
            vals_to_include,
            as_dict,
        )

    def m_u_parameters_chart(self):
        """Display a chart of the m and u parameters of the linkage model

        Examples:
            ```py
            linker.m_u_parameters_chart()
            ```
            To view offline (if you don't have an internet connection):
            ```py
            from splink.charts import save_offline_chart
            c = linker.match_weights_chart()
            save_offline_chart(c.to_dict(), "test_chart.html")
            ```
            View resultant html file in Jupyter (or just load it in your browser)
            ```py
            from IPython.display import IFrame
            IFrame(src="./test_chart.html", width=1000, height=500)
            ```

        Returns:
            altair.Chart: An altair chart
        """

        return self._settings_obj.m_u_parameters_chart()

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
            df_predict (SplinkDataFrame): The outputs of `linker.predict()`
            df_clustered (SplinkDataFrame): The outputs of
                `linker.cluster_pairwise_predictions_at_threshold()`
            out_path (str): The path (including filename) to save the html file to.
            sampling_method (str, optional): `random`, `by_cluster_size` or
                `lowest_density_clusters`. Defaults to `random`.
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
            df_p = linker.predict()
            df_c = linker.cluster_pairwise_predictions_at_threshold(df_p, 0.5)
            linker.cluster_studio_dashboard(
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
        self._raise_error_if_necessary_waterfall_columns_not_computed()

        rendered = render_splink_cluster_studio_html(
            self,
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

    def save_model_to_json(
        self, out_path: str | None = None, overwrite: bool = False
    ) -> dict[str, Any]:
        """Save the configuration and parameters of the linkage model to a `.json` file.

        The model can later be loaded back in using `linker.load_model()`.
        The settings dict is also returned in case you want to save it a different way.

        Examples:
            ```py
            linker.save_model_to_json("my_settings.json", overwrite=True)
            ```
        Args:
            out_path (str, optional): File path for json file. If None, don't save to
                file. Defaults to None.
            overwrite (bool, optional): Overwrite if already exists? Defaults to False.

        Returns:
            dict: The settings as a dictionary.
        """
        model_dict = self._settings_obj.as_dict()
        if out_path:
            if os.path.isfile(out_path) and not overwrite:
                raise ValueError(
                    f"The path {out_path} already exists. Please provide a different "
                    "path or set overwrite=True"
                )
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(model_dict, f, indent=4)
        return model_dict

    def estimate_probability_two_random_records_match(
        self,
        deterministic_matching_rules: List[Union[str, BlockingRuleCreator]],
        recall: float,
        max_rows_limit: int = int(1e9),
    ) -> None:
        """Estimate the model parameter `probability_two_random_records_match` using
        a direct estimation approach.

        See [here](https://github.com/moj-analytical-services/splink/issues/462)
        for discussion of methodology

        Args:
            deterministic_matching_rules (list): A list of deterministic matching
                rules that should be designed to admit very few (none if possible)
                false positives
            recall (float): A guess at the recall the deterministic matching rules
                will attain.  i.e. what proportion of true matches will be recovered
                by these deterministic rules
        """

        if (recall > 1) or (recall <= 0):
            raise ValueError(
                f"Estimated recall must be greater than 0 "
                f"and no more than 1. Supplied value {recall}."
            ) from None

        deterministic_matching_rules = ensure_is_iterable(deterministic_matching_rules)
        blocking_rules: List[BlockingRule] = []
        for br in deterministic_matching_rules:
            blocking_rules.append(
                to_blocking_rule_creator(br).get_blocking_rule(
                    self.db_api.sql_dialect.name
                )
            )

        pd_df = _cumulative_comparisons_to_be_scored_from_blocking_rules(
            splink_df_dict=self._input_tables_dict,
            blocking_rules=blocking_rules,
            link_type=self._settings_obj._link_type,
            db_api=self.db_api,
            max_rows_limit=max_rows_limit,
            unique_id_column_name=self._settings_obj.column_info_settings.unique_id_column_name,
            source_dataset_column_name=self._settings_obj.column_info_settings.source_dataset_column_name,
        )

        records = pd_df.to_dict(orient="records")

        summary_record = records[-1]
        num_observed_matches = summary_record["cumulative_rows"]
        num_total_comparisons = summary_record["cartesian"]

        if num_observed_matches > num_total_comparisons * recall:
            raise ValueError(
                f"Deterministic matching rules led to more "
                f"observed matches than is consistent with supplied recall. "
                f"With these rules, recall must be at least "
                f"{num_observed_matches/num_total_comparisons:,.2f}."
            )

        num_expected_matches = num_observed_matches / recall
        prob = num_expected_matches / num_total_comparisons

        # warn about boundary values, as these will usually be in error
        if num_observed_matches == 0:
            logger.warning(
                f"WARNING: Deterministic matching rules led to no observed matches! "
                f"This means that no possible record pairs are matches, "
                f"and no records are linked to one another.\n"
                f"If this is truly the case then you do not need "
                f"to run the linkage model.\n"
                f"However this is usually in error; "
                f"expected rules to have recall of {100*recall:,.0f}%. "
                f"Consider revising rules as they may have an error."
            )
        if prob == 1:
            logger.warning(
                "WARNING: Probability two random records match is estimated to be 1.\n"
                "This means that all possible record pairs are matches, "
                "and all records are linked to one another.\n"
                "If this is truly the case then you do not need "
                "to run the linkage model.\n"
                "However, it is more likely that this estimate is faulty. "
                "Perhaps your deterministic matching rules include "
                "too many false positives?"
            )

        self._settings_obj._probability_two_random_records_match = prob

        reciprocal_prob = "Infinity" if prob == 0 else f"{1/prob:,.2f}"
        logger.info(
            f"Probability two random records match is estimated to be  {prob:.3g}.\n"
            f"This means that amongst all possible pairwise record comparisons, one in "
            f"{reciprocal_prob} are expected to match.  "
            f"With {num_total_comparisons:,.0f} total"
            " possible comparisons, we expect a total of around "
            f"{num_expected_matches:,.2f} matching pairs"
        )

    def invalidate_cache(self):
        """Invalidate the Splink cache.  Any previously-computed tables
        will be recomputed.
        This is useful, for example, if the input data tables have changed.
        """

        # Nothing to delete
        if len(self._intermediate_table_cache) == 0:
            return

        # Before Splink executes a SQL command, it checks the cache to see
        # whether a table already exists with the name of the output table

        # This function has the effect of changing the names of the output tables
        # to include a different unique id

        # As a result, any previously cached tables will not be found
        self._cache_uid = ascii_uid(8)

        # Drop any existing splink tables from the database
        # Note, this is not actually necessary, it's just good housekeeping
        self.delete_tables_created_by_splink_from_db()

        # As a result, any previously cached tables will not be found
        self._intermediate_table_cache.invalidate_cache()

    def register_table_input_nodes_concat_with_tf(self, input_data, overwrite=False):
        """Register a pre-computed version of the input_nodes_concat_with_tf table that
        you want to re-use e.g. that you created in a previous run

        This method allowed you to register this table in the Splink cache
        so it will be used rather than Splink computing this table anew.

        Args:
            input_data: The data you wish to register. This can be either a dictionary,
                pandas dataframe, pyarrow table or a spark dataframe.
            overwrite (bool): Overwrite the table in the underlying database if it
                exists
        """

        table_name_physical = "__splink__df_concat_with_tf_" + self._cache_uid
        splink_dataframe = self.register_table(
            input_data, table_name_physical, overwrite=overwrite
        )
        splink_dataframe.templated_name = "__splink__df_concat_with_tf"

        self._intermediate_table_cache["__splink__df_concat_with_tf"] = splink_dataframe
        return splink_dataframe

    def register_table_predict(self, input_data, overwrite=False):
        table_name_physical = "__splink__df_predict_" + self._cache_uid
        splink_dataframe = self.register_table(
            input_data, table_name_physical, overwrite=overwrite
        )
        self._intermediate_table_cache["__splink__df_predict"] = splink_dataframe
        splink_dataframe.templated_name = "__splink__df_predict"
        return splink_dataframe

    def register_term_frequency_lookup(self, input_data, col_name, overwrite=False):
        input_col = InputColumn(
            col_name,
            column_info_settings=self._settings_obj.column_info_settings,
            sql_dialect=self._settings_obj._sql_dialect,
        )
        table_name_templated = colname_to_tf_tablename(input_col)
        table_name_physical = f"{table_name_templated}_{self._cache_uid}"
        splink_dataframe = self.register_table(
            input_data, table_name_physical, overwrite=overwrite
        )
        self._intermediate_table_cache[table_name_templated] = splink_dataframe
        splink_dataframe.templated_name = table_name_templated
        return splink_dataframe

    def register_labels_table(self, input_data, overwrite=False):
        table_name_physical = "__splink__df_labels_" + ascii_uid(8)
        splink_dataframe = self.register_table(
            input_data, table_name_physical, overwrite=overwrite
        )
        splink_dataframe.templated_name = "__splink__df_labels"
        return splink_dataframe

    def labelling_tool_for_specific_record(
        self,
        unique_id,
        source_dataset=None,
        out_path="labelling_tool.html",
        overwrite=False,
        match_weight_threshold=-4,
        view_in_jupyter=False,
        show_splink_predictions_in_interface=True,
    ):
        """Create a standalone, offline labelling dashboard for a specific record
        as identified by its unique id

        Args:
            unique_id (str): The unique id of the record for which to create the
                labelling tool
            source_dataset (str, optional): If there are multiple datasets, to
                identify the record you must also specify the source_dataset. Defaults
                to None.
            out_path (str, optional): The output path for the labelling tool. Defaults
                to "labelling_tool.html".
            overwrite (bool, optional): If true, overwrite files at the output
                path if they exist. Defaults to False.
            match_weight_threshold (int, optional): Include possible matches in the
                output which score above this threshold. Defaults to -4.
            view_in_jupyter (bool, optional): If you're viewing in the Jupyter
                html viewer, set this to True to extract your labels. Defaults to False.
            show_splink_predictions_in_interface (bool, optional): Whether to
                show information about the Splink model's predictions that could
                potentially bias the decision of the clerical labeller. Defaults to
                True.
        """

        df_comparisons = generate_labelling_tool_comparisons(
            self,
            unique_id,
            source_dataset,
            match_weight_threshold=match_weight_threshold,
        )

        render_labelling_tool_html(
            self,
            df_comparisons,
            show_splink_predictions_in_interface=show_splink_predictions_in_interface,
            out_path=out_path,
            view_in_jupyter=view_in_jupyter,
            overwrite=overwrite,
        )

    def _find_blocking_rules_below_threshold(
        self, max_comparisons_per_rule, blocking_expressions=None
    ):
        return find_blocking_rules_below_threshold_comparison_count(
            self, max_comparisons_per_rule, blocking_expressions
        )

    def _detect_blocking_rules_for_prediction(
        self,
        max_comparisons_per_rule,
        blocking_expressions=None,
        min_freedom=1,
        num_runs=200,
        num_equi_join_weight=0,
        field_freedom_weight=1,
        num_brs_weight=10,
        num_comparison_weight=10,
        return_as_df=False,
    ):
        """Find blocking rules for prediction below some given threshold of the
        maximum number of comparisons that can be generated per blocking rule
        (max_comparisons_per_rule).
        Uses a heuristic cost algorithm to identify the 'best' set of blocking rules
        Args:
            max_comparisons_per_rule (int): The maximum number of comparisons that
                each blocking rule is allowed to generate
            blocking_expressions: By default, blocking rules will be equi-joins
                on the columns used by the Splink model.  This allows you to manually
                specify sql expressions from which combinations will be created. For
                example, if you specify ["substr(dob, 1,4)", "surname", "dob"]
                blocking rules will be chosen by blocking on combinations
                of those expressions.
            min_freedom (int, optional): The minimum amount of freedom any column should
                be allowed.
            num_runs (int, optional): Each run selects rows using a heuristic and costs
                them. The more runs, the more likely you are to find the best rule.
                Defaults to 5.
            num_equi_join_weight (int, optional): Weight allocated to number of equi
                joins in the blocking rules.
                Defaults to 0 since this is cost better captured by other criteria.
            field_freedom_weight (int, optional): Weight given to the cost of
                having individual fields which don't havem much flexibility.  Assigning
                a high weight here makes it more likely you'll generate combinations of
                blocking rules for which most fields are allowed to vary more than
                the minimum. Defaults to 1.
            num_brs_weight (int, optional): Weight assigned to the cost of
                additional blocking rules.  Higher weight here will result in a
                 preference for fewer blocking rules. Defaults to 10.
            num_comparison_weight (int, optional): Weight assigned to the cost of
                larger numbers of comparisons, which happens when more of the blocking
                rules are close to the max_comparisons_per_rule.  A higher
                 weight here prefers sets of rules which generate lower total
                comparisons. Defaults to 10.
            return_as_df (bool, optional): If false, assign recommendation to settings.
                If true, return a dataframe containing details of the weights.
                Defaults to False.
        """

        df_br_below_thres = find_blocking_rules_below_threshold_comparison_count(
            self, max_comparisons_per_rule, blocking_expressions
        )

        blocking_rule_suggestions = suggest_blocking_rules(
            df_br_below_thres,
            min_freedom=min_freedom,
            num_runs=num_runs,
            num_equi_join_weight=num_equi_join_weight,
            field_freedom_weight=field_freedom_weight,
            num_brs_weight=num_brs_weight,
            num_comparison_weight=num_comparison_weight,
        )

        if return_as_df:
            return blocking_rule_suggestions
        else:
            if blocking_rule_suggestions is None or len(blocking_rule_suggestions) == 0:
                logger.warning("No set of blocking rules found within constraints")
            else:
                suggestion = blocking_rule_suggestions[
                    "suggested_blocking_rules_as_splink_brs"
                ].iloc[0]
                self._settings_obj._blocking_rules_to_generate_predictions = suggestion

                suggestion_str = blocking_rule_suggestions[
                    "suggested_blocking_rules_for_prediction"
                ].iloc[0]
                msg = (
                    "The following blocking_rules_to_generate_predictions were "
                    "automatically detected and assigned to your settings:\n"
                )
                logger.info(f"{msg}{suggestion_str}")

    def _detect_blocking_rules_for_em_training(
        self,
        max_comparisons_per_rule,
        min_freedom=1,
        num_runs=200,
        num_equi_join_weight=0,
        field_freedom_weight=1,
        num_brs_weight=20,
        num_comparison_weight=10,
        return_as_df=False,
    ):
        """Find blocking rules for EM training below some given threshold of the
        maximum number of comparisons that can be generated per blocking rule
        (max_comparisons_per_rule).
        Uses a heuristic cost algorithm to identify the 'best' set of blocking rules
        Args:
            max_comparisons_per_rule (int): The maximum number of comparisons that
                each blocking rule is allowed to generate
            min_freedom (int, optional): The minimum amount of freedom any column should
                be allowed.
            num_runs (int, optional): Each run selects rows using a heuristic and costs
                them.  The more runs, the more likely you are to find the best rule.
                Defaults to 5.
            num_equi_join_weight (int, optional): Weight allocated to number of equi
                joins in the blocking rules.
                Defaults to 0 since this is cost better captured by other criteria.
                Defaults to 0 since this is cost better captured by other criteria.
            field_freedom_weight (int, optional): Weight given to the cost of
                having individual fields which don't havem much flexibility.  Assigning
                a high weight here makes it more likely you'll generate combinations of
                blocking rules for which most fields are allowed to vary more than
                the minimum. Defaults to 1.
            num_brs_weight (int, optional): Weight assigned to the cost of
                additional blocking rules.  Higher weight here will result in a
                 preference for fewer blocking rules. Defaults to 10.
            num_comparison_weight (int, optional): Weight assigned to the cost of
                larger numbers of comparisons, which happens when more of the blocking
                rules are close to the max_comparisons_per_rule.  A higher
                 weight here prefers sets of rules which generate lower total
                comparisons. Defaults to 10.
            return_as_df (bool, optional): If false, return just the recommendation.
                If true, return a dataframe containing details of the weights.
                Defaults to False.
        """

        df_br_below_thres = find_blocking_rules_below_threshold_comparison_count(
            self, max_comparisons_per_rule
        )

        blocking_rule_suggestions = suggest_blocking_rules(
            df_br_below_thres,
            min_freedom=min_freedom,
            num_runs=num_runs,
            num_equi_join_weight=num_equi_join_weight,
            field_freedom_weight=field_freedom_weight,
            num_brs_weight=num_brs_weight,
            num_comparison_weight=num_comparison_weight,
        )

        if return_as_df:
            return blocking_rule_suggestions
        else:
            if blocking_rule_suggestions is None or len(blocking_rule_suggestions) == 0:
                logger.warning("No set of blocking rules found within constraints")
                return None
            else:
                suggestion_str = blocking_rule_suggestions[
                    "suggested_EM_training_statements"
                ].iloc[0]
                msg = "The following EM training strategy was detected:\n"
                msg = f"{msg}{suggestion_str}"
                logger.info(msg)
                suggestion = blocking_rule_suggestions[
                    "suggested_blocking_rules_as_splink_brs"
                ].iloc[0]
                return suggestion
