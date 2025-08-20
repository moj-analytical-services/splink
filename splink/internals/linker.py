from __future__ import annotations

import logging
from copy import copy, deepcopy
from pathlib import Path
from statistics import median
from typing import Any, Dict, List, Optional, Sequence

from splink.internals.blocking import (
    BlockingRule,
    block_using_rules_sqls,
)
from splink.internals.cache_dict_with_logging import CacheDictWithLogging
from splink.internals.comparison_vector_values import (
    compute_comparison_vector_values_from_id_pairs_sqls,
)
from splink.internals.database_api import AcceptableInputTableType, DatabaseAPISubClass
from splink.internals.dialects import SplinkDialect
from splink.internals.em_training_session import EMTrainingSession
from splink.internals.exceptions import SplinkException
from splink.internals.find_brs_with_comparison_counts_below_threshold import (
    find_blocking_rules_below_threshold_comparison_count,
)
from splink.internals.input_column import InputColumn
from splink.internals.linker_components.clustering import LinkerClustering
from splink.internals.linker_components.evaluation import LinkerEvalution
from splink.internals.linker_components.inference import LinkerInference
from splink.internals.linker_components.misc import LinkerMisc
from splink.internals.linker_components.table_management import LinkerTableManagement
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
from splink.internals.unique_id_concat import (
    _composite_unique_id_from_edges_sql,
)
from splink.internals.vertically_concatenate import (
    compute_df_concat_with_tf,
    concat_table_column_names,
)

logger = logging.getLogger(__name__)


class Linker:
    """The Linker object manages the data linkage process and holds the data linkage
    model.

    Most of Splink's functionality can  be accessed by calling methods (functions)
    on the linker, such as `linker.inference.predict()`, `linker.profile_columns()` etc.

    The Linker class is intended for subclassing for specific backends, e.g.
    a `DuckDBLinker`.
    """

    def __init__(
        self,
        input_table_or_tables: str | list[str],
        settings: SettingsCreator | dict[str, Any] | Path | str,
        db_api: DatabaseAPISubClass,
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
            settings_dict (dict | Path | str): A Splink settings dictionary,
                or a path (either as a pathlib.Path object, or a string) to a json file
                defining a settings dictionary or pre-trained model.
            db_api (DatabaseAPI): A `DatabaseAPI` object, which manages interactions
                with the database. You can import these for use from
                `splink.backends.{your_backend}`
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

        self._db_api = db_api

        # TODO: temp hack for compat
        self._intermediate_table_cache: CacheDictWithLogging = (
            self._db_api._intermediate_table_cache
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
            db_api.sql_dialect.sql_dialect_str
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

        self._debug_mode = False

        self.clustering: "LinkerClustering" = LinkerClustering(self)
        self.evaluation: "LinkerEvalution" = LinkerEvalution(self)
        self.inference: "LinkerInference" = LinkerInference(self)
        self.misc: "LinkerMisc" = LinkerMisc(self)
        self.table_management: "LinkerTableManagement" = LinkerTableManagement(self)
        self.training: "LinkerTraining" = LinkerTraining(self)
        self.visualisations: "LinkerVisualisations" = LinkerVisualisations(self)

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
    def _concat_table_column_names(self) -> list[str]:
        """
        Returns the columns actually present in __splink__df_concat table.
        Includes source dataset name if it's been created, and logic of additional
        columns already taken care of
        """
        return concat_table_column_names(self)

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
    def _debug_mode(self) -> bool:
        return self._db_api.debug_mode

    @_debug_mode.setter
    def _debug_mode(self, value: bool) -> None:
        self._db_api.debug_mode = value

    # TODO: rename these!
    @property
    def _sql_dialect_str(self) -> str:
        return self._db_api.sql_dialect.sql_dialect_str

    @property
    def _sql_dialect(self) -> SplinkDialect:
        return self._db_api.sql_dialect

    @property
    def _infinity_expression(self):
        return self._sql_dialect.infinity_expression

    def _random_sample_sql(
        self, proportion, sample_size, seed=None, table=None, unique_id=None
    ):
        return self._sql_dialect.random_sample_sql(
            proportion, sample_size, seed=seed, table=table, unique_id=unique_id
        )

    def _register_input_tables(
        self,
        input_tables: Sequence[AcceptableInputTableType],
        input_aliases: Optional[str | List[str]],
    ) -> Dict[str, SplinkDataFrame]:
        input_tables_list = ensure_is_list(input_tables)

        if input_aliases is None:
            input_table_aliases = [
                f"__splink__input_table_{i}" for i, _ in enumerate(input_tables_list)
            ]
            overwrite = True
        else:
            input_table_aliases = ensure_is_list(input_aliases)
            overwrite = False

        return self._db_api.register_multiple_tables(
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
            settings_dialect=self._settings_obj._sql_dialect_str,
            linker_dialect=self._sql_dialect_str,
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
        return self._db_api.table_to_splink_dataframe(templated_name, physical_name)

    def __deepcopy__(self, memo):
        """When we do EM training, we need a copy of the linker which is independent
        of the main linker e.g. setting parameters on the copy will not affect the
        main linker.  This method implements ensures linker can be deepcopied.
        """
        new_linker = copy(self)
        new_linker._em_training_sessions = []

        new_settings = deepcopy(self._settings_obj)
        new_linker._settings_obj = new_settings

        new_linker.clustering = LinkerClustering(new_linker)
        new_linker.evaluation = LinkerEvalution(new_linker)
        new_linker.inference = LinkerInference(new_linker)
        new_linker.misc = LinkerMisc(new_linker)
        new_linker.table_management = LinkerTableManagement(new_linker)
        new_linker.training = LinkerTraining(new_linker)
        new_linker.visualisations = LinkerVisualisations(new_linker)

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
                if cl._has_estimated_u_values and not cl._fix_u_probability:
                    cl.u_probability = cl._trained_u_median
                if cl._has_estimated_m_values and not cl._fix_m_probability:
                    cl.m_probability = cl._trained_m_median

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
            f"{uid_l} = {uid_r}", sql_dialect_str=self._sql_dialect.sql_dialect_str
        )

        pipeline = CTEPipeline()
        nodes_with_tf = compute_df_concat_with_tf(self, pipeline)

        pipeline = CTEPipeline([nodes_with_tf])

        sqls = block_using_rules_sqls(
            input_tablename_l="__splink__df_concat_with_tf",
            input_tablename_r="__splink__df_concat_with_tf",
            blocking_rules=[blocking_rule],
            link_type="self_link",
            source_dataset_input_column=settings.column_info_settings.source_dataset_input_column,
            unique_id_input_column=settings.column_info_settings.unique_id_input_column,
        )
        pipeline.enqueue_list_of_sqls(sqls)

        blocked_pairs = self._db_api.sql_pipeline_to_splink_dataframe(pipeline)

        pipeline = CTEPipeline([blocked_pairs, nodes_with_tf])

        sqls = compute_comparison_vector_values_from_id_pairs_sqls(
            settings._columns_to_select_for_blocking,
            settings._columns_to_select_for_comparison_vector_values,
            input_tablename_l="__splink__df_concat_with_tf",
            input_tablename_r="__splink__df_concat_with_tf",
            source_dataset_input_column=settings.column_info_settings.source_dataset_input_column,
            unique_id_input_column=settings.column_info_settings.unique_id_input_column,
        )
        pipeline.enqueue_list_of_sqls(sqls)

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

        predictions = self._db_api.sql_pipeline_to_splink_dataframe(pipeline)

        return predictions

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

    def _find_blocking_rules_below_threshold(
        self, max_comparisons_per_rule, blocking_expressions=None, max_results=None
    ):
        return find_blocking_rules_below_threshold_comparison_count(
            self, max_comparisons_per_rule, blocking_expressions, max_results
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
