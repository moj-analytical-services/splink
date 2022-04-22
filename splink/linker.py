import logging
from copy import copy, deepcopy
from statistics import median
import hashlib

from .charts import (
    match_weight_histogram,
    missingness_chart,
    precision_recall_chart,
    roc_chart,
    parameter_estimate_comparisons,
)

from .blocking import block_using_rules_sql
from .comparison_vector_values import compute_comparison_vector_values_sql
from .em_training_session import EMTrainingSession
from .misc import bayes_factor_to_prob, escape_columns, prob_to_bayes_factor
from .predict import predict_from_comparison_vectors_sql
from .settings import Settings
from .term_frequencies import (
    compute_all_term_frequencies_sqls,
    term_frequencies_for_single_column_sql,
    colname_to_tf_tablename,
    join_tf_to_input_df,
)
from .profile_data import profile_columns
from .missingness import missingness_data

from .m_training import estimate_m_values_from_label_column
from .u_training import estimate_u_values
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

logger = logging.getLogger(__name__)


class SplinkDataFrame:
    """Abstraction over dataframe to handle basic operations
    like retrieving columns, which need different implementations
    depending on whether it's a spark dataframe, sqlite table etc.
    """

    def __init__(self, templated_name, physical_name):
        self.templated_name = templated_name
        self.physical_name = physical_name

    @property
    def columns(self):
        pass

    @property
    def columns_escaped(self):
        cols = self.columns
        return escape_columns(cols)

    def validate():
        pass

    def random_sample_sql(percent):
        pass

    @property
    def physical_and_template_names_equal(self):
        return self.templated_name == self.physical_name

    def _check_drop_table_created_by_splink(self, force_non_splink_table=False):

        if not self.physical_name.startswith("__splink__"):
            if not force_non_splink_table:
                raise ValueError(
                    f"You've asked to drop table {self.physical_name} from your "
                    "database which is not a table created by Splink.  If you really "
                    "want to drop this table, you can do so by setting "
                    "force_non_splink_table=True"
                )
        logger.debug(
            f"Dropping table with templated name {self.templated_name} and "
            f"physical name {self.physical_name}"
        )

    def drop_table_from_database(self, force_non_splink_table=False):
        raise NotImplementedError(
            "Drop table from database not implemented for this linker"
        )

    def as_record_dict(self, limit=None):
        pass

    def as_pandas_dataframe(self, limit=None):
        import pandas as pd

        return pd.DataFrame(self.as_record_dict(limit=limit))


class Linker:
    def __init__(self, settings_dict=None, input_tables={}, set_up_basic_logging=True):

        self.pipeline = SQLPipeline()

        self.settings_dict = settings_dict
        if settings_dict is None:
            self._settings_obj = None
        else:
            self._settings_obj = Settings(settings_dict)

        self.input_dfs = self._get_input_dataframe_dict(input_tables)

        self._validate_input_dfs()
        self.em_training_sessions = []

        self.names_of_tables_created_by_splink = []

        self.find_new_matches_mode = False
        self.train_u_using_random_sample_mode = False

        self.compare_two_records_mode = False

        self.output_schema = ""

        self.debug_mode = False

        if set_up_basic_logging:
            logging.basicConfig(
                format="%(message)s",
            )
            splink_logger = logging.getLogger("splink")
            splink_logger.setLevel(logging.INFO)

    @property
    def settings_obj(self):
        if self._settings_obj is None:
            raise ValueError(
                "You did not provide a settings dictionary when you "
                "created the linker.  To continue, you need to provide a settings "
                "dictionary using the `initialise_settings()` method on your linker "
                "object. i.e. linker.initialise_settings(settings_dict)"
            )
        return self._settings_obj

    def initialise_settings(self, settings_dict):
        self.settings_dict = settings_dict
        self._settings_obj = Settings(settings_dict)
        self._validate_input_dfs()

    @property
    def _input_tablename_l(self):

        if self.find_new_matches_mode:
            return "__splink__df_concat_with_tf"

        if self.compare_two_records_mode:
            return "__splink__compare_two_records_left_with_tf"

        if self.train_u_using_random_sample_mode:
            return "__splink__df_concat_with_tf_sample"

        if self.two_dataset_link_only:
            return "__splink_df_concat_with_tf_left"
        return "__splink__df_concat_with_tf"

    @property
    def _input_tablename_r(self):

        if self.find_new_matches_mode:
            return "__splink__df_new_records_with_tf"

        if self.compare_two_records_mode:
            return "__splink__compare_two_records_right_with_tf"

        if self.train_u_using_random_sample_mode:
            return "__splink__df_concat_with_tf_sample"

        if self.two_dataset_link_only:
            return "__splink_df_concat_with_tf_right"
        return "__splink__df_concat_with_tf"

    @property
    def two_dataset_link_only(self):
        # Two dataset link only join is a special case where an inner join of the
        # two datasets is much more efficient than self-joining the vertically
        # concatenation of all input datasets
        if self.find_new_matches_mode:
            return True

        if self.compare_two_records_mode:
            return True

        if len(self.input_dfs) == 2 and self.settings_obj._link_type == "link_only":
            return True
        else:
            return False

    def _prepend_schema_to_table_name(self, table_name):
        if self.output_schema:
            return f"{self.output_schema}.{table_name}"
        return table_name

    def _initialise_df_concat(self, materialise=True):
        sql = vertically_concatente_sql(self)
        self.enqueue_sql(sql, "__splink__df_concat")
        self.execute_sql_pipeline(materialise_as_hash=False)

    def _initialise_df_concat_with_tf(self, materialise=True):
        if self.table_exists_in_database("__splink__df_concat_with_tf"):
            return
        sql = vertically_concatente_sql(self)
        self.enqueue_sql(sql, "__splink__df_concat")

        sqls = compute_all_term_frequencies_sqls(self)
        for sql in sqls:
            self.enqueue_sql(sql["sql"], sql["output_table_name"])

        if self.two_dataset_link_only:
            # If we do not materialise __splink_df_concat_with_tf
            # we'd have to run all the code up to this point twice
            self.execute_sql_pipeline(materialise_as_hash=False)

            source_dataset_col = self.settings_obj._source_dataset_column_name
            # Need df_l to be the one with the lowest id to preeserve the property
            # that the left dataset is the one with the lowest concatenated id
            keys = self.input_dfs.keys()
            keys = list(sorted(keys))
            df_l = self.input_dfs[keys[0]]
            df_r = self.input_dfs[keys[1]]

            sql = f"""
            select * from __splink__df_concat_with_tf
            where {source_dataset_col} = '{df_l.templated_name}'
            """
            self.enqueue_sql(sql, "__splink_df_concat_with_tf_left")
            self.execute_sql_pipeline(materialise_as_hash=False)

            sql = f"""
            select * from __splink__df_concat_with_tf
            where {source_dataset_col} = '{df_r.templated_name}'
            """
            self.enqueue_sql(sql, "__splink_df_concat_with_tf_right")
            self.execute_sql_pipeline(materialise_as_hash=False)
        else:
            if materialise:
                self.execute_sql_pipeline(materialise_as_hash=False)

    def compute_tf_table(self, column_name):
        sql = vertically_concatente_sql(self)
        self.enqueue_sql(sql, "__splink__df_concat")
        sql = term_frequencies_for_single_column_sql(column_name)
        self.enqueue_sql(sql, colname_to_tf_tablename(column_name))
        return self.execute_sql_pipeline(materialise_as_hash=False)

    def enqueue_sql(self, sql, output_table_name):
        self.pipeline.enqueue_sql(sql, output_table_name)

    def execute_sql_pipeline(
        self,
        input_dataframes=[],
        materialise_as_hash=True,
        use_cache=True,
        transpile=True,
    ):

        if not self.debug_mode:
            sql_gen = self.pipeline._generate_pipeline(input_dataframes)

            output_tablename_templated = self.pipeline.queue[-1].output_table_name

            dataframe = self.sql_to_dataframe(
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

                dataframe = self.sql_to_dataframe(
                    sql,
                    output_tablename,
                    materialise_as_hash=False,
                    use_cache=False,
                    transpile=transpile,
                )

            return dataframe

    def sql_to_dataframe(
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

            if self.table_exists_in_database(output_tablename_templated):
                logger.debug(f"Using existing table {output_tablename_templated}")
                return self._df_as_obj(
                    output_tablename_templated, output_tablename_templated
                )

            if self.table_exists_in_database(table_name_hash):
                logger.debug(
                    f"Using cache for {output_tablename_templated}"
                    f" with physical name {table_name_hash}"
                )
                return self._df_as_obj(output_tablename_templated, table_name_hash)

        if self.debug_mode:
            print(sql)

        if materialise_as_hash:
            dataframe = self.execute_sql(
                sql, output_tablename_templated, table_name_hash, transpile=transpile
            )
        else:
            dataframe = self.execute_sql(
                sql,
                output_tablename_templated,
                output_tablename_templated,
                transpile=transpile,
            )

        self.names_of_tables_created_by_splink.append(dataframe.physical_name)

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
        new_linker.em_training_sessions = []
        new_settings = deepcopy(self.settings_obj)
        new_linker._settings_obj = new_settings
        return new_linker

    def _get_input_dataframe_dict(self, df_dict):
        d = {}
        for df_name, df_value in df_dict.items():
            d[df_name] = self._df_as_obj(df_name, df_value)
        return d

    def _get_input_tf_dict(self, df_dict):
        d = {}
        for df_name, df_value in df_dict.items():
            renamed = colname_to_tf_tablename(df_name)
            d[renamed] = self._df_as_obj(renamed, df_value)
        return d

    def execute_sql(self, sql, templated_name, physical_name, transpile=True):
        raise NotImplementedError(f"execute_sql not implemented for {type(self)}")

    def table_exists_in_database(self, table_name):
        raise NotImplementedError(
            f"table_exists_in_database not implemented for {type(self)}"
        )

    def _validate_input_dfs(self):
        for df in self.input_dfs.values():
            df.validate()

        if self._settings_obj is not None:
            if self.settings_obj._link_type == "dedupe_only":
                if len(self.input_dfs) > 1:
                    raise ValueError(
                        'If link_type = "dedupe only" then input tables must contain'
                        "only a single input table",
                    )

    def deterministic_link(self, return_df_as_value=True):

        df_dict = block_using_rules_sql(self)
        if return_df_as_value:
            return df_dict["__splink__df_blocked"].df_value
        else:
            return df_dict

    def train_u_using_random_sampling(self, target_rows):
        self._initialise_df_concat_with_tf(materialise=True)
        estimate_u_values(self, target_rows)
        self.populate_m_u_from_trained_values()

        self.settings_obj.columns_without_estimated_parameters_message()

    def train_m_from_label_column(self, label_colname):
        self._initialise_df_concat_with_tf(materialise=True)
        estimate_m_values_from_label_column(self, self.input_dfs, label_colname)
        self.populate_m_u_from_trained_values()

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

        self.populate_m_u_from_trained_values()

        self.populate_proportion_of_matches_from_trained_values()

        self.settings_obj.columns_without_estimated_parameters_message()

        return em_training_session

    def populate_proportion_of_matches_from_trained_values(self):
        # Need access to here to the individual training session
        # their blocking rules and m and u values
        prop_matches_estimates = []
        for em_training_session in self.em_training_sessions:
            training_lambda = em_training_session.settings_obj._proportion_of_matches
            training_lambda_bf = prob_to_bayes_factor(training_lambda)
            reverse_levels = (
                em_training_session.comparison_levels_to_reverse_blocking_rule
            )

            for reverse_level in reverse_levels:

                # Get comparison level on current settings obj
                cc = self.settings_obj._get_comparison_by_name(
                    reverse_level.comparison.comparison_name
                )

                cl = cc.get_comparison_level_by_comparison_vector_value(
                    reverse_level.comparison_vector_value
                )

                if cl.has_estimated_values:
                    bf = cl.trained_m_median / cl.trained_u_median
                else:
                    bf = cl.bayes_factor

                training_lambda_bf = training_lambda_bf / bf
            p = bayes_factor_to_prob(training_lambda_bf)
            prop_matches_estimates.append(p)

        self.settings_obj._proportion_of_matches = median(prop_matches_estimates)

    def populate_m_u_from_trained_values(self):
        ccs = self.settings_obj.comparisons

        for cc in ccs:
            for cl in cc.comparison_levels_excluding_null:
                if cl.has_estimated_u_values:
                    cl.u_probability = cl.trained_u_median
                if cl.has_estimated_m_values:
                    cl.m_probability = cl.trained_m_median

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

    def predict(self):

        # If the user only calls predict, it runs as a single pipeline with no
        # materialisation of anything
        self._initialise_df_concat_with_tf(materialise=False)

        sql = block_using_rules_sql(self)
        self.enqueue_sql(sql, "__splink__df_blocked")

        sql = compute_comparison_vector_values_sql(self.settings_obj)
        self.enqueue_sql(sql, "__splink__df_comparison_vectors")

        sqls = predict_from_comparison_vectors_sql(self.settings_obj)
        for sql in sqls:
            self.enqueue_sql(sql["sql"], sql["output_table_name"])

        predictions = self.execute_sql_pipeline([])
        self._predict_warning()
        return predictions

    def records_to_table(records, as_table_name):
        # Create table in database containing records
        # Probably quite difficult to implement correctly
        # Due to data type issues.
        raise NotImplementedError

    def find_matches_to_new_records(
        self, records, blocking_rules=None, match_weight_threshold=-4
    ):

        original_blocking_rules = (
            self.settings_obj._blocking_rules_to_generate_predictions
        )
        original_link_type = self.settings_obj._link_type

        self.records_to_table(records, "__splink__df_new_records")

        if blocking_rules is not None:
            self.settings_obj._blocking_rules_to_generate_predictions = blocking_rules
        self.settings_obj._link_type = "link_only_find_matches_to_new_records"
        self.find_new_matches_mode = True

        sql = join_tf_to_input_df(self.settings_obj)
        sql = sql.replace("__splink__df_concat", "__splink__df_new_records")
        self.enqueue_sql(sql, "__splink__df_new_records_with_tf")

        sql = block_using_rules_sql(self)
        self.enqueue_sql(sql, "__splink__df_blocked")

        sql = compute_comparison_vector_values_sql(self.settings_obj)
        self.enqueue_sql(sql, "__splink__df_comparison_vectors")

        sqls = predict_from_comparison_vectors_sql(self.settings_obj)
        for sql in sqls:
            self.enqueue_sql(sql["sql"], sql["output_table_name"])

        sql = f"""
        select * from __splink__df_predict
        where match_weight > {match_weight_threshold}
        """

        self.enqueue_sql(sql, "__splink_find_matches_predictions")

        predictions = self.execute_sql_pipeline(use_cache=False)

        self.settings_obj._blocking_rules_to_generate_predictions = (
            original_blocking_rules
        )
        self.settings_obj._link_type = original_link_type
        self.find_new_matches_mode = False

        return predictions

    def compare_two_records(self, record_1, record_2):
        original_blocking_rules = (
            self.settings_obj._blocking_rules_to_generate_predictions
        )
        original_link_type = self.settings_obj._link_type

        self.compare_two_records_mode = True
        self.settings_obj._blocking_rules_to_generate_predictions = []

        self.records_to_table([record_1], "__splink__compare_two_records_left")
        self.records_to_table([record_2], "__splink__compare_two_records_right")

        sql_join_tf = join_tf_to_input_df(self.settings_obj)
        sql_join_tf = sql_join_tf.replace(
            "__splink__df_concat", "__splink__compare_two_records_left"
        )
        self.enqueue_sql(sql_join_tf, "__splink__compare_two_records_left_with_tf")

        sql_join_tf = sql_join_tf.replace(
            "__splink__compare_two_records_left", "__splink__compare_two_records_right"
        )
        self.enqueue_sql(sql_join_tf, "__splink__compare_two_records_right_with_tf")

        sql = block_using_rules_sql(self)
        self.enqueue_sql(sql, "__splink__df_blocked")

        sql = compute_comparison_vector_values_sql(self.settings_obj)
        self.enqueue_sql(sql, "__splink__df_comparison_vectors")

        sqls = predict_from_comparison_vectors_sql(self.settings_obj)
        for sql in sqls:
            self.enqueue_sql(sql["sql"], sql["output_table_name"])

        predictions = self.execute_sql_pipeline(use_cache=False)

        self.settings_obj._blocking_rules_to_generate_predictions = (
            original_blocking_rules
        )
        self.settings_obj._link_type = original_link_type
        self.compare_two_records_mode = False

        return predictions

    def delete_tables_created_by_splink_from_db(
        self, retain_term_frequency=True, retain_df_concat_with_tf=True
    ):
        tables_remaining = []
        for name in self.names_of_tables_created_by_splink:
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

        self.names_of_tables_created_by_splink = tables_remaining

    def profile_columns(self, column_expressions, top_n=10, bottom_n=10):

        return profile_columns(self, column_expressions, top_n=top_n, bottom_n=bottom_n)

    def train_m_from_pairwise_labels(self, table_name):
        self._initialise_df_concat_with_tf(materialise=True)
        estimate_m_from_pairwise_labels(self, table_name)

    def roc_from_labels(self, labels_tablename):
        df_truth_space = truth_space_table(self, labels_tablename)
        recs = df_truth_space.as_record_dict()
        return roc_chart(recs)

    def precision_recall_from_labels(self, labels_tablename):
        df_truth_space = truth_space_table(self, labels_tablename)
        recs = df_truth_space.as_record_dict()
        return precision_recall_chart(recs)

    def truth_space_table(self, labels_tablename):
        return truth_space_table(self, labels_tablename)

    def match_weight_histogram(self, df_predict, target_bins=30, width=600, height=250):
        df = histogram_data(self, df_predict, target_bins)
        recs = df.as_record_dict()
        return match_weight_histogram(recs, width=width, height=height)

    def splink_comparison_viewer(
        self, df_predict, out_path, overwrite=False, num_example_rows=2
    ):
        svd_sql = comparison_vector_distribution_sql(self)
        self.enqueue_sql(svd_sql, "__splink__df_comparison_vector_distribution")

        sqls = comparison_viewer_table(self, num_example_rows)
        for sql in sqls:
            self.enqueue_sql(sql["sql"], sql["output_table_name"])

        df = self.execute_sql_pipeline([df_predict])

        render_splink_comparison_viewer_html(
            df.as_record_dict(),
            self.settings_obj.as_completed_dict,
            out_path,
            overwrite,
        )

    def parameter_estimate_comparisons(self, include_m=True, include_u=True):
        records = self.settings_obj._parameter_estimates_as_records

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

    def _predict_warning(self):

        if not self.settings_obj.is_fully_trained:
            msg = (
                "\n -- WARNING --\n"
                "You have called predict(), but there are some parameter "
                "estimates which have neither been estimated or specified in your "
                "settings dictionary.  To produce predictions the following"
                " untrained trained parameters will use default values."
            )
            messages = self.settings_obj.not_trained_messages()

            warn_message = "\n".join([msg] + messages)

            logger.warn(warn_message)
