import logging
from copy import copy, deepcopy
from statistics import median
import hashlib

from .blocking import block_using_rules
from .comparison_vector_values import compute_comparison_vector_values
from .em_training import EMTrainingSession
from .misc import bayes_factor_to_prob, escape_columns, prob_to_bayes_factor
from .predict import predict
from .settings import Settings
from .term_frequencies import (
    term_frequencies,
    sql_gen_term_frequencies,
    colname_to_tf_tablename,
    join_tf_to_input_df,
)
from .profile_data import profile_columns

from .m_training import estimate_m_values_from_label_column
from .u_training import estimate_u_values
from .pipeline import SQLPipeline

from .vertically_concatenate import vertically_concatente

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

    def as_record_dict(self):
        pass

    def as_pandas_dataframe(self):
        import pandas as pd

        return pd.DataFrame(self.as_record_dict())


class Linker:
    def __init__(self, settings_dict=None, input_tables={}):

        self.pipeline = SQLPipeline()
        self.initialise_settings(settings_dict)

        self.input_dfs = self._get_input_dataframe_dict(input_tables)

        self._validate_input_dfs()
        self.em_training_sessions = []

        self.names_of_tables_created_by_splink = []

        self.find_new_matches_mode = False
        self.train_u_using_random_sample_mode = False
        self.compare_two_records_mode = False

        self.debug_mode = False

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
        if settings_dict is None:
            self._settings_obj = None
        else:
            self._settings_obj = Settings(settings_dict)

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

    def _initialise_df_concat(self, materialise=True):
        sql = vertically_concatente(self.input_dfs)
        self.enqueue_sql(sql, "__splink__df_concat")
        self.execute_sql_pipeline(materialise_as_hash=False)

    def _initialise_df_concat_with_tf(self, materialise=True):
        if self.table_exists_in_database("__splink__df_concat_with_tf"):
            return
        sql = vertically_concatente(self.input_dfs)
        self.enqueue_sql(sql, "__splink__df_concat")

        sqls = term_frequencies(self)
        for sql in sqls:
            self.enqueue_sql(sql["sql"], sql["output_table_name"])

        if self.two_dataset_link_only:
            # If we do not materialise __splink_df_concat_with_tf
            # we'd have to run all the code up to this point twice
            self.execute_sql_pipeline(materialise_as_hash=False)

            source_dataset_col = self.settings_obj._source_dataset_column_name
            df_l, df_r = list(self.input_dfs.values())

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
        sql = vertically_concatente(self.input_dfs)
        self.enqueue_sql(sql, "__splink__df_concat")
        sql = sql_gen_term_frequencies(column_name)
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
        hash = "__splink__" + hash

        if use_cache:
            if self.table_exists_in_database(output_tablename_templated):
                return self._df_as_obj(
                    output_tablename_templated, output_tablename_templated
                )

            if self.table_exists_in_database(hash):
                return self._df_as_obj(output_tablename_templated, hash)

        if self.debug_mode:
            print(sql)

        if materialise_as_hash:
            dataframe = self.execute_sql(
                sql, output_tablename_templated, hash, transpile=transpile
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

    def deterministic_link(self, return_df_as_value=True):

        df_dict = block_using_rules(self)
        if return_df_as_value:
            return df_dict["__splink__df_blocked"].df_value
        else:
            return df_dict

    def train_u_using_random_sampling(self, target_rows):
        self._initialise_df_concat_with_tf(materialise=True)
        estimate_u_values(self, target_rows)
        self.populate_m_u_from_trained_values()

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

            global_prop_matches_fully_trained = True
            for reverse_level in reverse_levels:

                # Get comparison level on current settings obj
                cc = self.settings_obj._get_comparison_by_name(
                    reverse_level.comparison.comparison_name
                )

                cl = cc.get_comparison_level_by_comparison_vector_value(
                    reverse_level.comparison_vector_value
                )

                if cl.is_trained:
                    bf = cl.trained_m_median / cl.trained_u_median
                else:
                    bf = cl.bayes_factor
                    global_prop_matches_fully_trained = False

                training_lambda_bf = training_lambda_bf / bf
            p = bayes_factor_to_prob(training_lambda_bf)
            prop_matches_estimates.append(p)

        if not global_prop_matches_fully_trained:
            print(
                "Proportion of matches not fully trained, "
                f"current estimates are {prop_matches_estimates}"
            )
        else:
            print(
                "Proportion of matches can now be estimated, "
                f"estimates are {prop_matches_estimates}"
            )

        self.settings_obj._proportion_of_matches = median(prop_matches_estimates)

    def populate_m_u_from_trained_values(self):
        ccs = self.settings_obj.comparisons

        for cc in ccs:
            for cl in cc.comparison_levels:
                if cl.u_is_trained:
                    cl.u_probability = cl.trained_u_median
                if cl.m_is_trained:
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

        sql = block_using_rules(self)
        self.enqueue_sql(sql, "__splink__df_blocked")

        sql = compute_comparison_vector_values(self.settings_obj)
        self.enqueue_sql(sql, "__splink__df_comparison_vectors")

        sqls = predict(self.settings_obj)
        for sql in sqls:
            self.enqueue_sql(sql["sql"], sql["output_table_name"])

        predictions = self.execute_sql_pipeline([])
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
        self.settings_obj._link_type = "link_only"
        self.find_new_matches_mode = True

        sql = join_tf_to_input_df(self.settings_obj)
        sql = sql.replace("__splink__df_concat", "__splink__df_new_records")
        self.enqueue_sql(sql, "__splink__df_new_records_with_tf")

        sql = block_using_rules(self)
        self.enqueue_sql(sql, "__splink__df_blocked")

        sql = compute_comparison_vector_values(self.settings_obj)
        self.enqueue_sql(sql, "__splink__df_comparison_vectors")

        sqls = predict(self.settings_obj)
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

        sql = block_using_rules(self)
        self.enqueue_sql(sql, "__splink__df_blocked")

        sql = compute_comparison_vector_values(self.settings_obj)
        self.enqueue_sql(sql, "__splink__df_comparison_vectors")

        sqls = predict(self.settings_obj)
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
