import logging
from copy import copy, deepcopy
from statistics import median
import hashlib

from typing import Union, List

from .charts import (
    match_weight_histogram,
    missingness_chart,
    precision_recall_chart,
    roc_chart,
    parameter_estimate_comparisons,
    waterfall_chart,
)

from .blocking import block_using_rules_sql
from .comparison_vector_values import compute_comparison_vector_values_sql
from .em_training_session import EMTrainingSession
from .misc import bayes_factor_to_prob, prob_to_bayes_factor, ensure_is_list
from .predict import predict_from_comparison_vectors_sql
from .settings import Settings
from .term_frequencies import (
    compute_all_term_frequencies_sqls,
    term_frequencies_for_single_column_sql,
    colname_to_tf_tablename,
    _join_tf_to_input_df,
)
from .profile_data import profile_columns
from .missingness import missingness_data

from .m_training import estimate_m_values_from_label_column
from .estimate_u import estimate_u_values
from .pipeline import SQLPipeline

from .vertically_concatenate import vertically_concatente_sql
from .m_from_labels import estimate_m_from_pairwise_labels
from .accuracy import roc_table

from .match_weight_histogram import histogram_data
from .comparison_vector_distribution import comparison_vector_distribution_sql
from .splink_comparison_viewer import (
    comparison_viewer_table,
    render_splink_comparison_viewer_html,
)
from .analyse_blocking import number_of_comparisons_generated_by_blocking_rule_sql

from .splink_dataframe import SplinkDataFrame

from .connected_components import (
    _cc_create_unique_id_cols,
    solve_connected_components,
)

from .cluster_studio import render_splink_cluster_studio_html

logger = logging.getLogger(__name__)


class Linker:
    """Manages the data linkage process and holds the data linkage model."""

    def __init__(
        self,
        input_table_or_tables: Union[str, list],
        settings_dict: dict = None,
        set_up_basic_logging: bool = True,
        input_table_aliases: Union[str, list] = None,
    ):
        """The Linker object manages the data linkage process and holds the data linkage
        model.

        Most of Splink's functionality can  be accessed by calling functions (methods)
        on the linker, such as `linker.predict()`, `linker.profile_columns()` etc.

        The Linker class is intended for subclassing for specific backends, e.g.
        a DuckDBLinker.

        Args:
            input_table_or_tables (Union[str, list]): Input data into the linkage model.
                Either a single string (the name of a table in a database) for
                deduplication jobs, or a list of strings  (the name of tables in a
                database) for link_only or link_and_dedupe
            settings_dict (dict, optional): A Splink settings dictionary. If not
                provided when the object is created, can later be added using
                `linker.initialise_settings()` Defaults to None.
            set_up_basic_logging (bool, optional): If true, sets ups up basic logging
                so that Splink sends messages at INFO level to stdout. Defaults to True.
            input_table_aliases (Union[str, list], optional): Labels assigned to
                input tables in Splink outputs.  If the names of the tables in the
                input database are long or unspecific, this argument can be used
                to attach more easily readable/interpretable names. Defaults to None.
        """

        self._pipeline = SQLPipeline()

        self._settings_dict = settings_dict
        if settings_dict is None:
            self._settings_obj_ = None
        else:
            self._settings_obj_ = Settings(settings_dict)

        self._input_tables_dict = self._get_input_tables_dict(
            input_table_or_tables, input_table_aliases
        )

        self._validate_input_dfs()
        self._em_training_sessions = []

        self._names_of_tables_created_by_splink = []

        self._find_new_matches_mode = False
        self._train_u_using_random_sample_mode = False
        self._compare_two_records_mode = False

        self._output_schema = ""

        self.debug_mode = False

        if set_up_basic_logging:
            logging.basicConfig(
                format="%(message)s",
            )
            splink_logger = logging.getLogger("splink")
            splink_logger.setLevel(logging.INFO)

    @property
    def _settings_obj(self) -> Settings:
        if self._settings_obj_ is None:
            raise ValueError(
                "You did not provide a settings dictionary when you "
                "created the linker.  To continue, you need to provide a settings "
                "dictionary using the `initialise_settings()` method on your linker "
                "object. i.e. linker.initialise_settings(settings_dict)"
            )
        return self._settings_obj_

    @property
    def _input_tablename_l(self):

        if self._find_new_matches_mode:
            return "__splink__df_concat_with_tf"

        if self._compare_two_records_mode:
            return "__splink__compare_two_records_left_with_tf"

        if self._train_u_using_random_sample_mode:
            return "__splink__df_concat_with_tf_sample"

        if self._two_dataset_link_only:
            return "__splink_df_concat_with_tf_left"
        return "__splink__df_concat_with_tf"

    @property
    def _input_tablename_r(self):

        if self._find_new_matches_mode:
            return "__splink__df_new_records_with_tf"

        if self._compare_two_records_mode:
            return "__splink__compare_two_records_right_with_tf"

        if self._train_u_using_random_sample_mode:
            return "__splink__df_concat_with_tf_sample"

        if self._two_dataset_link_only:
            return "__splink_df_concat_with_tf_right"
        return "__splink__df_concat_with_tf"

    @property
    def _two_dataset_link_only(self):
        # Two dataset link only join is a special case where an inner join of the
        # two datasets is much more efficient than self-joining the vertically
        # concatenation of all input datasets
        if self._find_new_matches_mode:
            return True

        if self._compare_two_records_mode:
            return True

        if (
            len(self._input_tables_dict) == 2
            and self._settings_obj._link_type == "link_only"
        ):
            return True
        else:
            return False

    def _prepend_schema_to_table_name(self, table_name):
        if self._output_schema:
            return f"{self._output_schema}.{table_name}"
        return table_name

    def _initialise_df_concat(self, materialise=True):
        if self._table_exists_in_database("__splink__df_concat"):
            return
        sql = vertically_concatente_sql(self)
        self._enqueue_sql(sql, "__splink__df_concat")
        self._execute_sql_pipeline(materialise_as_hash=False)

    def _initialise_df_concat_with_tf(self, materialise=True):
        if self._table_exists_in_database("__splink__df_concat_with_tf"):
            return
        sql = vertically_concatente_sql(self)
        self._enqueue_sql(sql, "__splink__df_concat")

        sqls = compute_all_term_frequencies_sqls(self)
        for sql in sqls:
            self._enqueue_sql(sql["sql"], sql["output_table_name"])

        if self._two_dataset_link_only:
            # If we do not materialise __splink_df_concat_with_tf
            # we'd have to run all the code up to this point twice
            self._execute_sql_pipeline(materialise_as_hash=False)

            source_dataset_col = self._settings_obj._source_dataset_column_name
            # Need df_l to be the one with the lowest id to preeserve the property
            # that the left dataset is the one with the lowest concatenated id
            keys = self._input_tables_dict.keys()
            keys = list(sorted(keys))
            df_l = self._input_tables_dict[keys[0]]
            df_r = self._input_tables_dict[keys[1]]

            sql = f"""
            select * from __splink__df_concat_with_tf
            where {source_dataset_col} = '{df_l.templated_name}'
            """
            self._enqueue_sql(sql, "__splink_df_concat_with_tf_left")
            self._execute_sql_pipeline(materialise_as_hash=False)

            sql = f"""
            select * from __splink__df_concat_with_tf
            where {source_dataset_col} = '{df_r.templated_name}'
            """
            self._enqueue_sql(sql, "__splink_df_concat_with_tf_right")
            self._execute_sql_pipeline(materialise_as_hash=False)
        else:
            if materialise:
                self._execute_sql_pipeline(materialise_as_hash=False)

    def _table_to_splink_dataframe(
        self, templated_name, physical_name
    ) -> SplinkDataFrame:
        """Create a SplinkDataframe from a table in the underlying database called
        `physical_name`.

        Associate a `templated_name` with this table, which signifies the purpose
        or 'meaning' of this table to splink. (e.g. `__splink__df_blocked`)

        Args:
            templated_name (str): The purpose of the table to Splink
            physical_name (str): The name of the table in the underlying databse
        """
        raise NotImplementedError(
            "_table_to_splink_dataframe not implemented on this linker"
        )

    def _enqueue_sql(self, sql, output_table_name):
        """Add sql to the current pipeline, but do not execute the pipeline."""
        self._pipeline.enqueue_sql(sql, output_table_name)

    def _execute_sql_pipeline(
        self,
        input_dataframes: List[SplinkDataFrame] = [],
        materialise_as_hash=True,
        use_cache=True,
        transpile=True,
    ) -> SplinkDataFrame:

        """Execute the SQL queued in the current pipeline as a single statement
        e.g. `with a as (), b as , c as (), select ... from c`, then execute the
        pipeline, returning the resultant table as a SplinkDataFrame

        Args:
            input_dataframes (List[SplinkDataFrame], optional): A 'starting point' of
                SplinkDataFrames if needed. Defaults to [].
            materialise_as_hash (bool, optional): If true, the output tablename will end
                in a unique identifer. Defaults to True.
            use_cache (bool, optional): If true, look at whether the SQL pipeline has
                been executed before, and if so, use the existing result. Defaults to
                True.
            transpile (bool, optional): Transpile the SQL using SQLGlot. Defaults to
                True.

        Returns:
            SplinkDataFrame: _description_
        """

        if not self.debug_mode:
            sql_gen = self._pipeline._generate_pipeline(input_dataframes)

            output_tablename_templated = self._pipeline.queue[-1].output_table_name

            dataframe = self._sql_to_splink_dataframe(
                sql_gen,
                output_tablename_templated,
                materialise_as_hash,
                use_cache,
                transpile,
            )
            return dataframe
        else:
            # In debug mode, we do not pipeline the sql and print the
            # results of each part of the pipeline
            for task in self._pipeline._generate_pipeline_parts(input_dataframes):
                output_tablename = task.output_table_name
                sql = task.sql
                print("------")
                print(f"--------Creating table: {output_tablename}--------")

                dataframe = self._sql_to_splink_dataframe(
                    sql,
                    output_tablename,
                    materialise_as_hash=False,
                    use_cache=False,
                    transpile=transpile,
                )

            return dataframe

    def _enqueue_and_execute_sql_pipeline(
        self,
        sql,
        output_table_name,
        materialise_as_hash=True,
        use_cache=True,
        transpile=True,
    ) -> SplinkDataFrame:

        """
        Wrapper method to enqueue and execute a sql pipeline
        in a single call.
        """

        self._enqueue_sql(sql, output_table_name)
        return self._execute_sql_pipeline([], materialise_as_hash, use_cache, transpile)

    def _sql_to_splink_dataframe(
        self,
        sql,
        output_tablename_templated,
        materialise_as_hash=True,
        use_cache=True,
        transpile=True,
    ) -> SplinkDataFrame:
        """Execute sql (or if identical sql has been run before, return cached results),
        reset pipeline, and return a splink dataframe representing the results of the
        sql"""

        self._pipeline.reset()

        hash = hashlib.sha256(sql.encode()).hexdigest()[:7]
        # Ensure hash is valid sql table name
        table_name_hash = f"{output_tablename_templated}_{hash}"

        if use_cache:

            if self._table_exists_in_database(output_tablename_templated):
                logger.debug(f"Using existing table {output_tablename_templated}")
                return self._table_to_splink_dataframe(
                    output_tablename_templated, output_tablename_templated
                )

            if self._table_exists_in_database(table_name_hash):
                logger.debug(
                    f"Using cache for {output_tablename_templated}"
                    f" with physical name {table_name_hash}"
                )
                return self._table_to_splink_dataframe(
                    output_tablename_templated, table_name_hash
                )

        if self.debug_mode:
            print(sql)

        if materialise_as_hash:
            splink_dataframe = self._execute_sql(
                sql, output_tablename_templated, table_name_hash, transpile=transpile
            )
        else:
            splink_dataframe = self._execute_sql(
                sql,
                output_tablename_templated,
                output_tablename_templated,
                transpile=transpile,
            )

        self._names_of_tables_created_by_splink.append(splink_dataframe.physical_name)

        if self.debug_mode:

            df_pd = splink_dataframe.as_pandas_dataframe()
            try:
                from IPython.display import display

                display(df_pd)
            except ModuleNotFoundError:
                print(df_pd)

        return splink_dataframe

    def __deepcopy__(self, memo):
        new_linker = copy(self)
        new_linker._em_training_sessions = []
        new_settings = deepcopy(self._settings_obj)
        new_linker._settings_obj_ = new_settings
        return new_linker

    def _ensure_aliases_populated_and_is_list(
        self, input_table_or_tables, input_table_aliases
    ):
        if input_table_aliases is None:
            input_table_aliases = input_table_or_tables

        if not isinstance(input_table_aliases, list):
            input_table_aliases = [input_table_aliases]
        return input_table_aliases

    def _get_input_tables_dict(self, input_table_or_tables, input_table_aliases):

        input_table_or_tables = ensure_is_list(input_table_or_tables)

        input_table_aliases = self._ensure_aliases_populated_and_is_list(
            input_table_or_tables, input_table_aliases
        )

        d = {}
        for table_name, table_alias in zip(input_table_or_tables, input_table_aliases):
            d[table_alias] = self._table_to_splink_dataframe(table_alias, table_name)
        return d

    def _get_input_tf_dict(self, df_dict):
        d = {}
        for df_name, df_value in df_dict.items():
            renamed = colname_to_tf_tablename(df_name)
            d[renamed] = self._table_to_splink_dataframe(renamed, df_value)
        return d

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

    def _execute_sql(self, sql, templated_name, physical_name, transpile=True):
        raise NotImplementedError(f"execute_sql not implemented for {type(self)}")

    def _table_exists_in_database(self, table_name):
        raise NotImplementedError(
            f"table_exists_in_database not implemented for {type(self)}"
        )

    def _validate_input_dfs(self):
        for df in self._input_tables_dict.values():
            df.validate()

        if self._settings_obj_ is not None:
            if self._settings_obj._link_type == "dedupe_only":
                if len(self._input_tables_dict) > 1:
                    raise ValueError(
                        'If link_type = "dedupe only" then input tables must contain'
                        "only a single input table",
                    )

    def _populate_proportion_of_matches_from_trained_values(self):
        # Need access to here to the individual training session
        # their blocking rules and m and u values
        prop_matches_estimates = []
        for em_training_session in self._em_training_sessions:
            training_lambda = em_training_session._settings_obj._proportion_of_matches
            training_lambda_bf = prob_to_bayes_factor(training_lambda)
            reverse_levels = (
                em_training_session._comparison_levels_to_reverse_blocking_rule
            )

            logger.log(
                15,
                "\n"
                f"Proportion of matches from trained model blocking on "
                f"{em_training_session._blocking_rule_for_training}: "
                f"{training_lambda:,.3f}",
            )

            for reverse_level in reverse_levels:

                # Get comparison level on current settings obj
                cc = self._settings_obj._get_comparison_by_output_column_name(
                    reverse_level.comparison._output_column_name
                )

                cl = cc._get_comparison_level_by_comparison_vector_value(
                    reverse_level._comparison_vector_value
                )

                if cl._has_estimated_values:
                    bf = cl._trained_m_median / cl._trained_u_median
                else:
                    bf = cl._bayes_factor

                logger.log(
                    15,
                    f"Reversing comparison level {cc._output_column_name}"
                    f" using bayes factor {bf:,.3f}",
                )

                training_lambda_bf = training_lambda_bf / bf

                logger.log(
                    15,
                    "This estimate of proportion of matches now: "
                    f" {bayes_factor_to_prob(training_lambda_bf):,.3f}",
                )
            p = bayes_factor_to_prob(training_lambda_bf)
            prop_matches_estimates.append(p)

        self._settings_obj._proportion_of_matches = median(prop_matches_estimates)
        logger.log(
            15,
            "\nMedian of prop of matches estimates: "
            f"{self._settings_obj._proportion_of_matches:,.3f}",
        )

    def _records_to_table(records, as_table_name):
        # Create table in database containing records
        # Probably quite difficult to implement correctly
        # Due to data type issues.
        raise NotImplementedError

    def _populate_m_u_from_trained_values(self):
        ccs = self._settings_obj.comparisons

        for cc in ccs:
            for cl in cc._comparison_levels_excluding_null:
                if cl._has_estimated_u_values:
                    cl.u_probability = cl._trained_u_median
                if cl._has_estimated_m_values:
                    cl.m_probability = cl._trained_m_median

    def _delete_tables_created_by_splink_from_db(
        self, retain_term_frequency=True, retain_df_concat_with_tf=True
    ):
        tables_remaining = []
        for name in self._names_of_tables_created_by_splink:
            # Only delete tables explicitly marked as having been created by splink
            if "__splink__" not in name:
                tables_remaining.append(name)
                continue
            if name == "__splink__df_concat_with_tf":
                if retain_df_concat_with_tf:
                    tables_remaining.append(name)
                else:
                    self._delete_table_from_database(name)
            elif name.startswith("__splink__df_tf_"):
                if retain_term_frequency:
                    tables_remaining.append(name)
                else:
                    self._delete_table_from_database(name)
            else:
                self._delete_table_from_database(name)

        self._names_of_tables_created_by_splink = tables_remaining

    def initialise_settings(self, settings_dict: dict):
        """Initialise settings for the linker.  To be used if settings were
        not passed to the linker on creation.

        Args:
            settings_dict (dict): A Splink settings dictionary
        """
        self._settings_dict = settings_dict
        self._settings_obj_ = Settings(settings_dict)
        self._validate_input_dfs()

    def compute_tf_table(self, column_name: str) -> SplinkDataFrame:
        """Compute a term frequency table for a given column and persist to the database

        This method is useful if you want to pre-compute term frequency tables e.g.
        so that real time linkage executes faster.

        Args:
            column_name (str): The column name in the input table

        Returns:
            SplinkDataFrame: The resultant table as a splink data frame
        """
        sql = vertically_concatente_sql(self)
        self._enqueue_sql(sql, "__splink__df_concat")
        sql = term_frequencies_for_single_column_sql(column_name)
        self._enqueue_sql(sql, colname_to_tf_tablename(column_name))
        return self._execute_sql_pipeline(materialise_as_hash=False)

    def deterministic_link(self) -> SplinkDataFrame:
        """Uses the blocking rules specified in the
        `blocking_rules_to_generate_predictions` of the settings dictionary to
        generate pairwise record comparisons.


        `blocking_rules_to_generate_predictions` contains a list of blocking rules
        which are strict enough to  generate only true links,  then the result
        will be a dataframe of true links.  This methodology, however, is likely to
        result in missed links (false negatives).

        Returns:
            SplinkDataFrame: A SplinkDataFrame of the pairwise comparisons.  This
                represents a table materialised in the database. Methods on the
                SplinkDataFrame allow you to access the underlying data.
        """
        self._initialise_df_concat_with_tf()
        sql = block_using_rules_sql(self)
        self._enqueue_sql(sql, "__splink__df_blocked")
        return self._execute_sql_pipeline()

    def estimate_u_using_random_sampling(self, target_rows: int):
        """Estimate the u parameters of the linkage model using random sampling i.e.
        amongst non matching records, what proportion of record comparisons fall
        into each comparison level.

        This procedure takes a sample of the data and generates the cartesian
        product of pairwise record comparisons amongst the sampled records.
        The validity of the u values rests on the assumption that the resultant
        pairwise comparisons are non-matches (or at least, they are very unlikely to be
        matches). For large datasets, this is typically true.

        Args:
            target_rows (int): The target number of pairwise record comparisons from
            which to derive the u values.  Larger will give more accurate estimates
            but lead to longer runtimes.  In our experience at least 1e9 (one billion)
            gives best results. 1e7 (ten million) is often adequate for rapid model
            development.

        Examples:
            >>> linker.estimate_u_using_random_sampling(1e9)

        Returns:
            Updates the estimated u parameters within the linker object
            and returns nothing.
        """
        self._initialise_df_concat_with_tf(materialise=True)
        estimate_u_values(self, target_rows)
        self._populate_m_u_from_trained_values()

        self._settings_obj._columns_without_estimated_parameters_message()

    def estimate_m_from_label_column(self, label_colname: str):
        """Estimate the m parameters of the linkage model from a label (ground truth)
        column in the input dataframe(s).

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
            >>> linker.estimate_m_from_label_column("social_security_number")

        Returns:
            Updates the estimated m parameters within the linker object
            and returns nothing.
        """
        self._initialise_df_concat_with_tf(materialise=True)
        estimate_m_values_from_label_column(
            self, self._input_tables_dict, label_colname
        )
        self._populate_m_u_from_trained_values()

        self._settings_obj._columns_without_estimated_parameters_message()

    def estimate_parameters_using_expectation_maximisation(
        self,
        blocking_rule: str,
        comparisons_to_deactivate: list = None,
        comparison_levels_to_reverse_blocking_rule: list = None,
        fix_proportion_of_matches: bool = False,
        fix_m_probabilities=False,
        fix_u_probabilities=True,
    ) -> EMTrainingSession:
        """Estimate the parameters of the linkage model using expectation maximisation

        By default, the m probabilities are estimate, but not the u probabilities,
        because good estiamtes for the u probabilities can be obtained from
        `linker.estimate_u_using_random_sampling()`.  This can be controlled using the
        `fix_u_probabilities` parameter.

        The blocking rule provided is used to generate pairwise record comparisons.

        By default, m parameters are estimated for all comparisons except those which
        are included in the blocking rule.

        For example, if the blocking rule is `l.first_name = r.first_name`, then
        parameter esimates will be made for all comparison except those which use
        `first_name` in their sql_condition

        By default, the proportion of matches is estimated for the blocked data, and
        then the m and u parameters for the columns specified in the blocking rules are
        used to estiamte the global proportion of matches.

        To control which comparisons should have their parameter estimated, and the
        process of 'reversing out' the global proportion of matches, the user
        may specify `comparisons_to_deactivate` and
        `comparison_levels_to_reverse_blocking_rule`.

        Args:
            blocking_rule (str): The blocking rule used to generate pairwise record
                comparisons.
            comparisons_to_deactivate (list, optional): By default, splink will
                analyse the blocking rule provided and estimate the m parameters for
                all comaprisons except those included in the blocking rule.  If
                comparisons_to_deactivate are provided, spink will instead
                estimate m parameters for all comparison except those specified by name
                in the comparisons_to_deactivate list.  Defaults to None.
            comparison_levels_to_reverse_blocking_rule (list, optional): By default,
                splink will analyse the blocking rule provided and adjust the
                global proportion of matches to account for the matches specified
                in the blocking rule. If provided, this argument will overrule
                this default behaviour. Defaults to None.
            fix_proportion_of_matches (bool, optional): If True, do not update the
                proportion of matches after each iteration. Defaults to False.
            fix_m_probabilities (bool, optional): If True, do not update the m
                probabilities after each iteration. Defaults to False.
            fix_u_probabilities (bool, optional): If True, do not update the u
                probabilities after each iteration. Defaults to True.

        Examples:
            >>> blocking_rule = "l.first_name = r.first_name and l.dob = r.dob"
            >>> linker.estimate_parameters_using_expectation_maximisation(blocking_rule)

        Returns:
            Updates the estimated m parameters and proportion_of_matches within the
            linker object and returns nothing.
        """

        self._initialise_df_concat_with_tf(materialise=True)
        em_training_session = EMTrainingSession(
            self,
            blocking_rule,
            fix_u_probabilities=fix_u_probabilities,
            fix_m_probabilities=fix_m_probabilities,
            fix_proportion_of_matches=fix_proportion_of_matches,
            comparisons_to_deactivate=comparisons_to_deactivate,
            comparison_levels_to_reverse_blocking_rule=comparison_levels_to_reverse_blocking_rule,  # noqa
        )

        em_training_session._train()

        self._populate_m_u_from_trained_values()

        self._populate_proportion_of_matches_from_trained_values()

        self._settings_obj._columns_without_estimated_parameters_message()

        return em_training_session

    def predict(
        self,
        threshold_match_probability: float = None,
        threshold_match_weight: float = None,
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

        Returns:
            SplinkDataFrame: A SplinkDataFrame of the pairwise comparisons.  This
                represents a table materialised in the database. Methods on the
                SplinkDataFrame allow you to access the underlying data.

        """

        # If the user only calls predict, it runs as a single pipeline with no
        # materialisation of anything
        self._initialise_df_concat_with_tf(materialise=False)

        sql = block_using_rules_sql(self)
        self._enqueue_sql(sql, "__splink__df_blocked")

        sql = compute_comparison_vector_values_sql(self._settings_obj)
        self._enqueue_sql(sql, "__splink__df_comparison_vectors")

        sqls = predict_from_comparison_vectors_sql(
            self._settings_obj, threshold_match_probability, threshold_match_weight
        )
        for sql in sqls:
            self._enqueue_sql(sql["sql"], sql["output_table_name"])

        predictions = self._execute_sql_pipeline()
        self._predict_warning()
        return predictions

    def find_matches_to_new_records(
        self, records: List[dict], blocking_rules=None, match_weight_threshold=-4
    ) -> SplinkDataFrame:
        """Given one or more records, find records in the input dataset(s) which match
        and return in order of the splink prediction score.

        i.e. this effectively provides a way of searching the input datasets
        for a given record

        Args:
            records (List[dict]): Input search record(s).
            blocking_rules (str, optional): Blocking rules to select
                which records to find and score. If None, do not use a blocking
                rule - meaning the input records will be compared to all records
                provided to the linker when it was instantiated. Defaults to None.
            match_weight_threshold (int, optional): Return matches with a match weight
                above this threshold. Defaults to -4.

        Returns:
            SplinkDataFrame: A SplinkDataFrame of the pairwise comparisons.  This
                represents a table materialised in the database. Methods on the
                SplinkDataFrame allow you to access the underlying data.
        """

        original_blocking_rules = (
            self._settings_obj._blocking_rules_to_generate_predictions
        )
        original_link_type = self._settings_obj._link_type

        self._records_to_table(records, "__splink__df_new_records")

        if blocking_rules is not None:
            self._settings_obj._blocking_rules_to_generate_predictions = blocking_rules
        self._settings_obj._link_type = "link_only_find_matches_to_new_records"
        self._find_new_matches_mode = True

        sql = _join_tf_to_input_df(self._settings_obj)
        sql = sql.replace("__splink__df_concat", "__splink__df_new_records")
        self._enqueue_sql(sql, "__splink__df_new_records_with_tf")

        sql = block_using_rules_sql(self)
        self._enqueue_sql(sql, "__splink__df_blocked")

        sql = compute_comparison_vector_values_sql(self._settings_obj)
        self._enqueue_sql(sql, "__splink__df_comparison_vectors")

        sqls = predict_from_comparison_vectors_sql(self._settings_obj)
        for sql in sqls:
            self._enqueue_sql(sql["sql"], sql["output_table_name"])

        sql = f"""
        select * from __splink__df_predict
        where match_weight > {match_weight_threshold}
        """

        self._enqueue_sql(sql, "__splink_find_matches_predictions")

        predictions = self._execute_sql_pipeline(use_cache=False)

        self._settings_obj._blocking_rules_to_generate_predictions = (
            original_blocking_rules
        )
        self._settings_obj._link_type = original_link_type
        self._find_new_matches_mode = False

        return predictions

    def compare_two_records(self, record_1: dict, record_2: dict):
        """Use the linkage model to compare and score two records

        Args:
            record_1 (dict): dictionary representing the first record.  Columns names
                and data types must be the same as the columns in the settings object
            record_2 (dict): dictionary representing the second record.  Columns names
                and data types must be the same as the columns in the settings object

        Returns:
            SplinkDataFrame: Pairwise comparison with scored prediction
        """
        original_blocking_rules = (
            self._settings_obj._blocking_rules_to_generate_predictions
        )
        original_link_type = self._settings_obj._link_type

        self._compare_two_records_mode = True
        self._settings_obj._blocking_rules_to_generate_predictions = []

        self._records_to_table([record_1], "__splink__compare_two_records_left")
        self._records_to_table([record_2], "__splink__compare_two_records_right")

        sql_join_tf = _join_tf_to_input_df(self._settings_obj)
        sql_join_tf = sql_join_tf.replace(
            "__splink__df_concat", "__splink__compare_two_records_left"
        )
        self._enqueue_sql(sql_join_tf, "__splink__compare_two_records_left_with_tf")

        sql_join_tf = sql_join_tf.replace(
            "__splink__compare_two_records_left", "__splink__compare_two_records_right"
        )
        self._enqueue_sql(sql_join_tf, "__splink__compare_two_records_right_with_tf")

        sql = block_using_rules_sql(self)
        self._enqueue_sql(sql, "__splink__df_blocked")

        sql = compute_comparison_vector_values_sql(self._settings_obj)
        self._enqueue_sql(sql, "__splink__df_comparison_vectors")

        sqls = predict_from_comparison_vectors_sql(self._settings_obj)
        for sql in sqls:
            self._enqueue_sql(sql["sql"], sql["output_table_name"])

        predictions = self._execute_sql_pipeline(use_cache=False)

        self._settings_obj._blocking_rules_to_generate_predictions = (
            original_blocking_rules
        )
        self._settings_obj._link_type = original_link_type
        self._compare_two_records_mode = False

        return predictions

    def cluster_pairwise_predictions_at_threshold(
        self, df_predict: SplinkDataFrame, threshold_match_probability: float
    ) -> SplinkDataFrame:
        """Clusters the pairwise match predictions that result from `linker.predict()`
        into groups of connected record using the connected components graph clustering
        algorithm

        Records with an estimated `match_probability` above
        `threshold_match_probability` are considered to be a match (i.e. they represent
        the same entity).

        Args:
            df_predict (SplinkDataFrame): The results of `linker.predict()`
            threshold_match_probability (float): Filter the pairwise match predictions
                to include only pairwise comparisons with a match_probability above this
                threshold. This dataframe is then fed into the clustering
                algorithm.

        Returns:
            SplinkDataFrame: A SplinkDataFrame containing a list of all IDs, clustered
                into groups based on the desired match threshold.

        """

        self._initialise_df_concat_with_tf(df_predict)

        edges_table = _cc_create_unique_id_cols(
            self,
            df_predict,
            threshold_match_probability,
        )

        cc = solve_connected_components(self, edges_table)

        return cc

    def delete_tables_created_by_splink_from_db(
        self, retain_term_frequency=True, retain_df_concat_with_tf=True
    ):
        tables_remaining = []
        current_tables = self._names_of_tables_created_by_splink
        for splink_df in current_tables:
            name = splink_df.templated_name
            # Only delete tables explicitly marked as having been created by splink
            if "__splink__" not in name:
                tables_remaining.append(splink_df)
                continue
            if name == "__splink__df_concat_with_tf":
                if retain_df_concat_with_tf:
                    tables_remaining.append(splink_df)
                else:
                    self._delete_table_from_database(name)
            elif name.startswith("__splink__df_tf_"):
                if retain_term_frequency:
                    tables_remaining.append(splink_df)
                else:
                    self._delete_table_from_database(name)
            else:
                self._delete_table_from_database(name)

        self._names_of_tables_created_by_splink = tables_remaining

    def profile_columns(self, column_expressions, top_n=10, bottom_n=10):

        return profile_columns(self, column_expressions, top_n=top_n, bottom_n=bottom_n)

    def train_m_from_pairwise_labels(self, table_name):
        self._initialise_df_concat_with_tf(materialise=True)
        estimate_m_from_pairwise_labels(self, table_name)

    def roc_chart_from_labels(
        self,
        labels_tablename,
        threshold_actual=0.5,
        match_weight_round_to_nearest: float = None,
    ):
        """Generate a ROC chart from labelled (ground truth) data.

        The table of labels should be in the following format, and should be registered
        with your database:
        ```
        |source_dataset_l|unique_id_l|source_dataset_r|unique_id_r|clerical_match_score|
        |----------------|-----------|----------------|-----------|--------------------|
        |df_1            |1          |df_2            |2          |0.99                |
        |df_1            |1          |df_2            |3          |0.2                 |
        ```
        Note that `source_dataset` and `unique_id` should correspond to the values
        specified in the settings dict, and the `input_table_aliases` passed to the
        `linker` object.

        For `dedupe_only` links, the `source_dataset` columns can be ommitted.

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
            >>> # DuckDBLinker
            >>> labels = pd.read_csv("my_labels.csv")
            >>> linker._con.register("labels", labels)
            >>> linker.roc_chart_from_labels("labels")
            >>>
            >>> # SparkLinker
            >>> labels = spark.read.csv("my_labels.csv", header=True)
            >>> labels.createDataFrame("labels")
            >>> linker.roc_chart_from_labels("labels")

        Returns:
            SplinkDataFrame
        """
        df_truth_space = roc_table(
            self,
            labels_tablename,
            threshold_actual=threshold_actual,
            match_weight_round_to_nearest=match_weight_round_to_nearest,
        )
        recs = df_truth_space.as_record_dict()
        return roc_chart(recs)

    def precision_recall_chart_from_labels(self, labels_tablename):
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
            >>> # DuckDBLinker
            >>> labels = pd.read_csv("my_labels.csv")
            >>> linker._con.register("labels", labels)
            >>> linker.precision_recall_chart_from_labels("labels")
            >>>
            >>> # SparkLinker
            >>> labels = spark.read.csv("my_labels.csv", header=True)
            >>> labels.createDataFrame("labels")
            >>> linker.precision_recall_chart_from_labels("labels")

        Returns:
            SplinkDataFrame
        """
        df_truth_space = roc_table(self, labels_tablename)
        recs = df_truth_space.as_record_dict()
        return precision_recall_chart(recs)

    def roc_table_from_labels(
        self,
        labels_tablename,
        threshold_actual=0.5,
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
            >>> # DuckDBLinker
            >>> labels = pd.read_csv("my_labels.csv")
            >>> linker._con.register("labels", labels)
            >>> linker.roc_table_from_labels("labels")
            >>>
            >>> # SparkLinker
            >>> labels = spark.read.csv("my_labels.csv", header=True)
            >>> labels.createDataFrame("labels")
            >>> linker.roc_table_from_labels("labels")

        Returns:
            SplinkDataFrame
        """

        return roc_table(
            self,
            labels_tablename,
            threshold_actual=threshold_actual,
            match_weight_round_to_nearest=match_weight_round_to_nearest,
        )

    def match_weight_histogram(self, df_predict, target_bins=30, width=600, height=250):
        df = histogram_data(self, df_predict, target_bins)
        recs = df.as_record_dict()
        return match_weight_histogram(recs, width=width, height=height)

    def waterfall_chart(self, records, filter_nulls=True, as_dict=False):
        return waterfall_chart(records, self._settings_obj, filter_nulls, as_dict)

    def comparison_viewer_dashboard(
        self,
        df_predict: SplinkDataFrame,
        out_path: str,
        overwrite=False,
        num_example_rows=2,
    ):
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

        Examples:
            >>> df_predictions = linker.predict()
            >>> linker.splink_comparison_viewer(df_predictions, "scv.html", True,2)
            >>>
            >>> # Optionally, in Jupyter, you can display the results inline
            >>> # Otherwise you can just load the html file in your browser
            >>> from IPython.display import IFrame
            >>> IFrame(src="./scv.html", width="100%", height=1200)

        """
        sql = comparison_vector_distribution_sql(self)
        self._enqueue_sql(sql, "__splink__df_comparison_vector_distribution")

        sqls = comparison_viewer_table(self, num_example_rows)
        for sql in sqls:
            self._enqueue_sql(sql["sql"], sql["output_table_name"])

        df = self._execute_sql_pipeline([df_predict])

        render_splink_comparison_viewer_html(
            df.as_record_dict(),
            self._settings_obj.as_completed_dict(),
            out_path,
            overwrite,
        )

    def parameter_estimate_comparisons_chart(self, include_m=True, include_u=True):
        records = self._settings_obj._parameter_estimates_as_records

        to_retain = []
        if include_m:
            to_retain.append("m")
        if include_u:
            to_retain.append("u")

        records = [r for r in records if r["m_or_u"] in to_retain]

        return parameter_estimate_comparisons(records)

    def missingness_chart(self, input_dataset: str = None):
        """Generate a summary chart of the missingness (prevalence of nulls) of
        columns in the input datasets.  By default, missingness is assessed across
        all input datasets

        Args:
            input_dataset (str, optional): Name of one of the input tables in the
            database.  If provided, missingness will be computed for this table alone.
            Defaults to None.
        """
        records = missingness_data(self, input_dataset)
        return missingness_chart(records, input_dataset)

    def compute_number_of_comparisons_generated_by_blocking_rule(
        self, blocking_rule: str, link_type: str = None
    ) -> dict:
        """Compute the number of pairwise record comparisons that would be generated by
        a blocking rule

        Args:
            blocking_rule (str): The blocking rule to analyse
            link_type (str, optional): The link type.  This is needed only if the
                linker has not yet been provided with a settings dictionary.  Defaults
                to None.

        Examples:
            >>> br = "l.first_name = r.first_name"
            >>> linker.compute_number_of_comparisons_generated_by_blocking_rule(br)

        Returns:
            dict: A dictionray containing the number of comparisons generated by the
                blocking rule
        """

        sql = vertically_concatente_sql(self)
        self._enqueue_sql(sql, "__splink__df_concat")

        sql = number_of_comparisons_generated_by_blocking_rule_sql(
            self, blocking_rule, link_type
        )
        self._enqueue_sql(sql, "__splink__analyse_blocking_rule")
        res = self._execute_sql_pipeline().as_record_dict()[0]
        return res

    def match_weights_chart(self):
        """Display a chart of the match weights of the linkage model

        Examples:
            >>> linker.match_weights_chart()
            >>>
            >>> # To view offline (if you don't have an internet connection):
            >>>
            >>> from splink.charts import save_offline_chart
            >>> c = linker.match_weights_chart()
            >>> save_offline_chart(c.spec, "test_chart.html")
            >>>
            >>> # View resultant html file in Jupyter (or just load it in your browser)
            >>> from IPython.display import IFrame
            >>> IFrame(src="./test_chart.html", width=1000, height=500)
        """
        return self._settings_obj.match_weights_chart()

    def m_u_parameters_chart(self):
        """Display a chart of the m and u parameters of the linkage model

        Examples:
            >>> linker.m_u_parameters_chart()
            >>>
            >>> # To view offline (if you don't have an internet connection):
            >>>
            >>> from splink.charts import save_offline_chart
            >>> c = linker.match_weights_chart()
            >>> save_offline_chart(c.spec, "test_chart.html")
            >>>
            >>> # View resultant html file in Jupyter (or just load it in your browser)
            >>> from IPython.display import IFrame
            >>> IFrame(src="./test_chart.html", width=1000, height=500)
        """

        return self._settings_obj.m_u_parameters_chart()

    def cluster_studio_dashboard(
        self,
        df_predict: SplinkDataFrame,
        df_clustered: SplinkDataFrame,
        cluster_ids: list,
        out_path: str,
        overwrite=False,
    ):
        """Generate an interactive html visualization of the predicted cluster and
        save to `out_path`.

        Args:
            df_predict (SplinkDataFrame): The outputs of `linker.predict()`
            df_clustered (SplinkDataFrame): The outputs of
                `linker.cluster_pairwise_predictions_at_threshold()`
            cluster_ids (list): The IDs of the clusters that will be displayed in the
                dashboard
            out_path (str): The path (including filename) to save the html file to.
            overwrite (bool, optional): Overwrite the html file if it already exists?
                Defaults to False.

        Examples:
            >>> df_p = linker.predict()
            >>> df_c = linker.cluster_pairwise_predictions_at_threshold(df_p, 0.5)
            >>> linker.cluster_studio_dashboard(
            >>>     df_p, df_c, [0, 4, 7], "cluster_studio.html"
            >>> )
            >>>
            >>> # Optionally, in Jupyter, you can display the results inline
            >>> # Otherwise you can just load the html file in your browser
            >>> from IPython.display import IFrame
            >>> IFrame(src="./cluster_studio.html", width="100%", height=1200)

        """

        return render_splink_cluster_studio_html(
            self, df_predict, df_clustered, cluster_ids, out_path, overwrite=overwrite
        )
