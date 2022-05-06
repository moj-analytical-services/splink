import logging
from copy import copy, deepcopy
from statistics import median
import hashlib

from typing import Union

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
from .misc import bayes_factor_to_prob, prob_to_bayes_factor
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
from .accuracy import truth_space_table

from .match_weight_histogram import histogram_data
from .comparison_vector_distribution import comparison_vector_distribution_sql
from .splink_comparison_viewer import (
    comparison_viewer_table,
    render_splink_comparison_viewer_html,
)
from .analyse_blocking import analyse_blocking_rule_sql

from .splink_dataframe import SplinkDataFrame


logger = logging.getLogger(__name__)


class Linker:
    def __init__(
        self,
        input_table_or_tables: Union[str, list],
        settings_dict=None,
        set_up_basic_logging=True,
        input_table_aliases: Union[str, list] = None,
    ):

        self.pipeline = SQLPipeline()

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

    def initialise_settings(self, settings_dict):
        self._settings_dict = settings_dict
        self._settings_obj_ = Settings(settings_dict)
        self._validate_input_dfs()

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

    def _enqueue_sql(self, sql, output_table_name):
        self.pipeline.enqueue_sql(sql, output_table_name)

    def _execute_sql_pipeline(
        self,
        input_dataframes=[],
        materialise_as_hash=True,
        use_cache=True,
        transpile=True,
    ) -> SplinkDataFrame:

        if not self.debug_mode:
            sql_gen = self.pipeline._generate_pipeline(input_dataframes)

            output_tablename_templated = self.pipeline.queue[-1].output_table_name

            dataframe = self._sql_to_dataframe(
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
            for task in self.pipeline._generate_pipeline_parts(input_dataframes):
                output_tablename = task.output_table_name
                sql = task.sql
                print("------")
                print(f"--------Creating table: {output_tablename}--------")

                dataframe = self._sql_to_dataframe(
                    sql,
                    output_tablename,
                    materialise_as_hash=False,
                    use_cache=False,
                    transpile=transpile,
                )

            return dataframe

    def _sql_to_dataframe(
        self,
        sql,
        output_tablename_templated,
        materialise_as_hash=True,
        use_cache=True,
        transpile=True,
    ):

        self.pipeline.reset()

        hash = hashlib.sha256(sql.encode()).hexdigest()[:7]
        # Ensure hash is valid sql table name
        table_name_hash = f"{output_tablename_templated}_{hash}"

        if use_cache:

            if self._table_exists_in_database(output_tablename_templated):
                logger.debug(f"Using existing table {output_tablename_templated}")
                return self._df_as_obj(
                    output_tablename_templated, output_tablename_templated
                )

            if self._table_exists_in_database(table_name_hash):
                logger.debug(
                    f"Using cache for {output_tablename_templated}"
                    f" with physical name {table_name_hash}"
                )
                return self._df_as_obj(output_tablename_templated, table_name_hash)

        if self.debug_mode:
            print(sql)

        if materialise_as_hash:
            dataframe = self._execute_sql(
                sql, output_tablename_templated, table_name_hash, transpile=transpile
            )
        else:
            dataframe = self._execute_sql(
                sql,
                output_tablename_templated,
                output_tablename_templated,
                transpile=transpile,
            )

        self._names_of_tables_created_by_splink.append(dataframe.physical_name)

        if self.debug_mode:

            df_pd = dataframe.as_pandas_dataframe()
            try:
                from IPython.display import display

                display(df_pd)
            except ModuleNotFoundError:
                print(df_pd)

        return dataframe

    def __deepcopy__(self, memo):
        new_linker = copy(self)
        new_linker._em_training_sessions = []
        new_settings = deepcopy(self._settings_obj)
        new_linker._settings_obj_ = new_settings
        return new_linker

    def _coerce_to_list(self, input_table_or_tables):
        if not isinstance(input_table_or_tables, list):
            input_table_or_tables = [input_table_or_tables]

        return input_table_or_tables

    def _ensure_aliases_populated_and_is_list(
        self, input_table_or_tables, input_table_aliases
    ):
        if input_table_aliases is None:
            input_table_aliases = input_table_or_tables

        if not isinstance(input_table_aliases, list):
            input_table_aliases = [input_table_aliases]
        return input_table_aliases

    def _get_input_tables_dict(self, input_table_or_tables, input_table_aliases):

        input_table_or_tables = self._coerce_to_list(input_table_or_tables)

        input_table_aliases = self._ensure_aliases_populated_and_is_list(
            input_table_or_tables, input_table_aliases
        )

        d = {}
        for table_name, table_alias in zip(input_table_or_tables, input_table_aliases):
            d[table_alias] = self._df_as_obj(table_alias, table_name)
        return d

    def _get_input_tf_dict(self, df_dict):
        d = {}
        for df_name, df_value in df_dict.items():
            renamed = colname_to_tf_tablename(df_name)
            d[renamed] = self._df_as_obj(renamed, df_value)
        return d

    def _predict_warning(self):

        if not self._settings_obj.is_fully_trained:
            msg = (
                "\n -- WARNING --\n"
                "You have called predict(), but there are some parameter "
                "estimates which have neither been estimated or specified in your "
                "settings dictionary.  To produce predictions the following"
                " untrained trained parameters will use default values."
            )
            messages = self._settings_obj.not_trained_messages()

            warn_message = "\n".join([msg] + messages)

            logger.warn(warn_message)

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
                em_training_session.comparison_levels_to_reverse_blocking_rule
            )

            logger.log(
                15,
                "\n"
                f"Proportion of matches from trained model blocking on "
                f"{em_training_session.blocking_rule_for_training}: "
                f"{training_lambda:,.3f}",
            )

            for reverse_level in reverse_levels:

                # Get comparison level on current settings obj
                cc = self._settings_obj._get_comparison_by_name(
                    reverse_level.comparison.comparison_name
                )

                cl = cc.get_comparison_level_by_comparison_vector_value(
                    reverse_level.comparison_vector_value
                )

                if cl.has_estimated_values:
                    bf = cl.trained_m_median / cl.trained_u_median
                else:
                    bf = cl.bayes_factor

                logger.log(
                    15,
                    f"Reversing comparison level {cc.comparison_name}"
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
            for cl in cc.comparison_levels_excluding_null:
                if cl.has_estimated_u_values:
                    cl.u_probability = cl.trained_u_median
                if cl.has_estimated_m_values:
                    cl.m_probability = cl.trained_m_median

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
                    self.delete_table_from_database(name)
            elif name.startswith("__splink__df_tf_"):
                if retain_term_frequency:
                    tables_remaining.append(name)
                else:
                    self.delete_table_from_database(name)
            else:
                self.delete_table_from_database(name)

        self._names_of_tables_created_by_splink = tables_remaining

    def compute_tf_table(self, column_name):
        sql = vertically_concatente_sql(self)
        self._enqueue_sql(sql, "__splink__df_concat")
        sql = term_frequencies_for_single_column_sql(column_name)
        self._enqueue_sql(sql, colname_to_tf_tablename(column_name))
        return self._execute_sql_pipeline(materialise_as_hash=False)

    def deterministic_link(self, return_df_as_value=True):

        df_dict = block_using_rules_sql(self)
        if return_df_as_value:
            return df_dict["__splink__df_blocked"].df_value
        else:
            return df_dict

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
            but lead to longer runtimes.  In our experience at least1e9 gives
            best results. 1e7 is often adequate for rapid model development.

        Returns:
            Updates the u values within the linker object with their estimates
            and returns nothing.
        """
        self._initialise_df_concat_with_tf(materialise=True)
        estimate_u_values(self, target_rows)
        self._populate_m_u_from_trained_values()

        self._settings_obj.columns_without_estimated_parameters_message()

    def estimate_m_from_label_column(self, label_colname: str):
        """If there exists a column that contains a ground truth identifier, this can
        be used to generate record comparisons for truly matching records.

        For example, if the entity being matched is persons, and your input dataset(s)
        contain social security number, this could be used to estimate the m values
        for the model.

        Note that this column does not need to be fully populated.  A common case is
        where a unique identifier such as social security number is only partially
        populated.

        Args:
            label_colname (str): The name of the column containing the ground truth
                label in the input data.
        """
        self._initialise_df_concat_with_tf(materialise=True)
        estimate_m_values_from_label_column(
            self, self._input_tables_dict, label_colname
        )
        self._populate_m_u_from_trained_values()

        self._settings_obj.columns_without_estimated_parameters_message()

    def train_m_using_expectation_maximisation(
        self,
        blocking_rule,
        comparisons_to_deactivate=None,
        comparison_levels_to_reverse_blocking_rule=None,
        fix_proportion_of_matches=False,
        fix_u_probabilities=True,
        fix_m_probabilities=False,
    ):
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

        em_training_session.train()

        self._populate_m_u_from_trained_values()

        self._populate_proportion_of_matches_from_trained_values()

        self._settings_obj.columns_without_estimated_parameters_message()

        return em_training_session

    def train_m_and_u_using_expectation_maximisation(
        self,
        blocking_rule,
        fix_proportion_of_matches=False,
        comparisons_to_deactivate=None,
        fix_u_probabilities=False,
        fix_m_probabilities=False,
        comparison_levels_to_reverse_blocking_rule=None,
    ):
        return self.train_m_using_expectation_maximisation(
            blocking_rule,
            fix_proportion_of_matches=fix_proportion_of_matches,
            comparisons_to_deactivate=comparisons_to_deactivate,
            fix_u_probabilities=fix_u_probabilities,
            fix_m_probabilities=fix_m_probabilities,
            comparison_levels_to_reverse_blocking_rule=comparison_levels_to_reverse_blocking_rule,  # noqa
        )

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
                match_weight above this threshold.. Defaults to None.

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
        self, records, blocking_rules=None, match_weight_threshold=-4
    ):

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

    def compare_two_records(self, record_1, record_2):
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

    def profile_columns(self, column_expressions, top_n=10, bottom_n=10):

        return profile_columns(self, column_expressions, top_n=top_n, bottom_n=bottom_n)

    def train_m_from_pairwise_labels(self, table_name):
        self._initialise_df_concat_with_tf(materialise=True)
        estimate_m_from_pairwise_labels(self, table_name)

    def roc_chart_from_labels(
        self, labels_tablename, threshold_actual=0.5, match_weight_round_to_nearest=None
    ):
        df_truth_space = truth_space_table(
            self,
            labels_tablename,
            threshold_actual=threshold_actual,
            match_weight_round_to_nearest=match_weight_round_to_nearest,
        )
        recs = df_truth_space.as_record_dict()
        return roc_chart(recs)

    def precision_recall_chart_from_labels(self, labels_tablename):
        df_truth_space = truth_space_table(self, labels_tablename)
        recs = df_truth_space.as_record_dict()
        return precision_recall_chart(recs)

    def truth_space_table(
        self, labels_tablename, threshold_actual=0.5, match_weight_round_to_nearest=None
    ):
        return truth_space_table(
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

    def splink_comparison_viewer(
        self, df_predict, out_path, overwrite=False, num_example_rows=2
    ):
        svd_sql = comparison_vector_distribution_sql(self)
        self._enqueue_sql(svd_sql, "__splink__df_comparison_vector_distribution")

        sqls = comparison_viewer_table(self, num_example_rows)
        for sql in sqls:
            self._enqueue_sql(sql["sql"], sql["output_table_name"])

        df = self._execute_sql_pipeline([df_predict])

        render_splink_comparison_viewer_html(
            df.as_record_dict(),
            self._settings_obj.as_completed_dict,
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

    def missingness_chart(self, input_dataset=None):
        records = missingness_data(self, input_dataset)
        return missingness_chart(records, input_dataset)

    def analyse_blocking_rule(self, blocking_rule, link_type=None):

        sql = vertically_concatente_sql(self)
        self._enqueue_sql(sql, "__splink__df_concat")

        sql = analyse_blocking_rule_sql(self, blocking_rule, link_type)
        self._enqueue_sql(sql, "__splink__analyse_blocking_rule")
        res = self._execute_sql_pipeline().as_record_dict()[0]
        return res

    def match_weights_chart(self):
        return self._settings_obj.match_weights_chart()

    def m_u_values_chart(self):
        return self._settings_obj.m_u_values_chart()
