from __future__ import annotations

import json
import logging
import os
from copy import copy, deepcopy
from pathlib import Path
from statistics import median
from typing import Any, Dict, List, Literal, Optional, Sequence, Union

from splink.internals.accuracy import (
    prediction_errors_from_label_column,
    prediction_errors_from_labels_table,
    truth_space_table_from_labels_column,
    truth_space_table_from_labels_table,
)
from splink.internals.blocking import (
    BlockingRule,
    block_using_rules_sqls,
    blocking_rule_to_obj,
    materialise_exploded_id_tables,
)
from splink.internals.blocking_rule_creator import BlockingRuleCreator
from splink.internals.cache_dict_with_logging import CacheDictWithLogging
from splink.internals.charts import (
    ChartReturnType,
    accuracy_chart,
    precision_recall_chart,
    roc_chart,
    threshold_selection_tool,
    unlinkables_chart,
)
from splink.internals.comparison_vector_values import (
    compute_comparison_vector_values_sql,
)
from splink.internals.connected_components import (
    _cc_create_unique_id_cols,
    solve_connected_components,
)
from splink.internals.database_api import AcceptableInputTableType, DatabaseAPISubClass
from splink.internals.dialects import SplinkDialect
from splink.internals.edge_metrics import compute_edge_metrics
from splink.internals.em_training_session import EMTrainingSession
from splink.internals.exceptions import SplinkException
from splink.internals.find_brs_with_comparison_counts_below_threshold import (
    find_blocking_rules_below_threshold_comparison_count,
)
from splink.internals.find_matches_to_new_records import (
    add_unique_id_and_source_dataset_cols_if_needed,
)
from splink.internals.graph_metrics import (
    GraphMetricsResults,
    _node_degree_sql,
    _size_density_centralisation_sql,
)
from splink.internals.input_column import InputColumn
from splink.internals.labelling_tool import (
    generate_labelling_tool_comparisons,
    render_labelling_tool_html,
)
from splink.internals.linker_components.training import LinkerTraining
from splink.internals.linker_components.visualisations import LinkerVisualisations
from splink.internals.misc import (
    ascii_uid,
    bayes_factor_to_prob,
    ensure_is_list,
    prob_to_bayes_factor,
)
from splink.internals.optimise_cost_of_brs import suggest_blocking_rules
from splink.internals.pipeline import CTEPipeline
from splink.internals.predict import (
    predict_from_comparison_vectors_sqls,
    predict_from_comparison_vectors_sqls_using_settings,
)
from splink.internals.settings_creator import SettingsCreator
from splink.internals.settings_validation.log_invalid_columns import (
    InvalidColumnsLogger,
    SettingsColumnCleaner,
)
from splink.internals.settings_validation.valid_types import (
    _validate_dialect,
)
from splink.internals.splink_dataframe import SplinkDataFrame
from splink.internals.term_frequencies import (
    _join_new_table_to_df_concat_with_tf_sql,
    colname_to_tf_tablename,
    term_frequencies_for_single_column_sql,
)
from splink.internals.unique_id_concat import (
    _composite_unique_id_from_edges_sql,
    _composite_unique_id_from_nodes_sql,
)
from splink.internals.unlinkables import unlinkables_data
from splink.internals.vertically_concatenate import (
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

        self.visualisations = LinkerVisualisations(self)
        self.training = LinkerTraining(self)

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
            self,
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
        self._raise_error_if_necessary_accuracy_columns_not_computed()
        df_truth_space = truth_space_table_from_labels_table(
            self,
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
            self,
            label_colname,
            include_false_positives,
            include_false_negatives,
            threshold,
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
