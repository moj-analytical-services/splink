import logging
from copy import copy, deepcopy
from statistics import median
import re
from string import ascii_lowercase
from itertools import chain

from .blocking import block_using_rules
from .comparison_vector_values import compute_comparison_vector_values
from .em_training import EMTrainingSession
from .misc import bayes_factor_to_prob, escape_columns, prob_to_bayes_factor
from .predict import predict
from .settings import Settings
from .term_frequencies import (
    colname_to_tf_tablename,
    join_tf_to_input_df,
    term_frequencies_dict,
)
from .m_training import estimate_m_values_from_label_column
from .u_training import estimate_u_values

from .vertically_concatenate import vertically_concatente

logger = logging.getLogger(__name__)


class SplinkDataFrame:
    """Abstraction over dataframe to handle basic operations
    like retrieving columns, which need different implementations
    depending on whether it's a spark dataframe, sqlite table etc.
    """

    def __init__(self, df_name, df_value):
        self.df_name = df_name
        self.df_value = df_value

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

    def as_record_dict(self):
        pass


class Linker:
    def __init__(self, settings_dict, input_tables, tf_tables={}):
        self.settings_dict = settings_dict

        self.settings_obj = Settings(settings_dict)
        self.input_dfs = self._get_input_dataframe_dict(input_tables)
        self.input_tf_tables = self._get_input_tf_dict(tf_tables)
        self._validate_input_dfs()
        self.em_training_sessions = []

        self.sql_tracker = {}  # track cached tables
        self.cache_queries = [
            "__splink__df_concat",
            "__splink__df_concat_with_tf",
            "__splink__df_blocked"
            ]

        sql_pipeline = vertically_concatente(self.input_dfs, self.generate_sql)
        sql_pipeline = self._add_term_frequencies(
            sql_pipeline=sql_pipeline
        )

    def __deepcopy__(self, memo):
        new_linker = copy(self)
        new_linker.em_training_sessions = []
        new_settings = deepcopy(self.settings_obj)
        new_linker.settings_obj = new_settings
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

    def _initialise_sql_pipeline(self, table_name):
        """
        Re-generate a pipeline. Should only be used on named tables.
        """
        return {"sql_pipe": [f"SELECT * FROM '{self.sql_tracker[table_name][0]}'"], "prev_dfs": [table_name]}

    def execute_sql(sql, df_dict, output_table_name):
        pass

    def _validate_input_dfs(self):
        for df in self.input_dfs.values():
            df.validate()

    def deterministic_link(self):

        sql_pipeline = self._initialise_sql_pipeline("__splink__df_concat_with_tf")
        sql_pipeline = block_using_rules(self.settings_obj, sql_pipeline, self.generate_sql)
        return self.execute_sql(sql_pipeline)  # this needs to be execute

    def _blocked_comparisons(self, sql_pipeline):

        sql_pipeline = block_using_rules(self.settings_obj, sql_pipeline, self.generate_sql)
        return sql_pipeline

    def _add_term_frequencies(self, sql_pipeline):

        # edit... actually, this might be ok???
        # if we just pass if there are no TF cols, then that should be fine...
        if not self.settings_obj._term_frequency_columns:  # test
            sql = "select * from __splink__df_concat"
            return self.generate_sql(
                sql, sql_pipeline,
                "__splink__df_concat_with_tf"
            )

        # Want to return tf tables as a dict of tables.
        sql_pipeline = term_frequencies_dict(
            self.settings_obj, sql_pipeline,
            self.input_tf_tables, self.generate_sql
        )

        # df_dict = {**df_dict, **tf_dict}
        df_cols = self.con.query(f"SELECT * FROM '{self.sql_tracker['__splink__df_concat'][0]}'").columns  # edit at some point as this is silly
        out = join_tf_to_input_df(
            self.settings_obj, df_cols,
            sql_pipeline, self.generate_sql
        )
        return out

    def comparison_vectors(self, sql_pipeline):
        sql_pipeline = self._blocked_comparisons(sql_pipeline)
        sql_pipeline = compute_comparison_vector_values(
            self.settings_obj, sql_pipeline, self.generate_sql
        )

        return sql_pipeline

    def train_u_using_random_sampling(self, target_rows):

        sql_pipeline = self._initialise_sql_pipeline("__splink__df_concat")
        estimate_u_values(self, sql_pipeline, target_rows)
        self.populate_m_u_from_trained_values()

    def train_m_from_label_column(self, label_colname):
        # migrate pls...
        sql_pipeline = self._initialise_sql_pipeline("__splink__df_concat")
        estimate_m_values_from_label_column(self, sql_pipeline, label_colname)
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

        em_training_session = EMTrainingSession(
            self,
            blocking_rule,
            fix_u_probabilities=fix_u_probabilities,
            fix_m_probabilities=fix_m_probabilities,
            fix_proportion_of_matches=fix_proportion_of_matches,
            comparisons_to_deactivate=comparisons_to_deactivate,
            comparison_levels_to_reverse_blocking_rule=comparison_levels_to_reverse_blocking_rule,
        )

        import time
        t = time.time()
        sql_pipeline = self._initialise_sql_pipeline("__splink__df_concat_with_tf")
        em_training_session.train(sql_pipeline)
        print("--- Train... %s seconds ---" % (time.time() - t))
        t = time.time()

        self.populate_m_u_from_trained_values()
        print("--- Populate_m_u stuff... %s seconds ---" % (time.time() - t))
        t = time.time()

        self.populate_proportion_of_matches_from_trained_values()
        print("--- populate_proportion... %s seconds ---" % (time.time() - t))
        t = time.time()

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
                f"Proportion of matches not fully trained, current estimates are {prop_matches_estimates}"
            )
        else:
            print(
                f"Proportion of matches can now be estimated, estimates are {prop_matches_estimates}"
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
            comparison_levels_to_reverse_blocking_rule=comparison_levels_to_reverse_blocking_rule,
        )

    def predict(self, return_df_as_value=True):
        sql_pipeline = self._initialise_sql_pipeline("__splink__df_concat_with_tf")
        sql_pipeline = self.comparison_vectors(sql_pipeline)
        sql_pipeline = predict(self.settings_obj, sql_pipeline, self.generate_sql)

        return self.execute_sql(sql_pipeline)

    def execute_sql(self):
        pass

    def combine_sql_queries(self, sql_pipeline):
        """
        Converts a given list of SQL queries into a singular query,
        bound together by WITH statements.
        Allows the backend SQL engine to perform optimisation steps where appropriate.
        """

        sql = sql_pipeline["sql_pipe"]
        table = sql_pipeline["prev_dfs"]

        if len(table)==0:
            return sql[0]

        sql_list = [f"{t} AS ({s})" for s, t in zip(sql, table)]
        sql_string = f'WITH {", ".join(sql_list)} SELECT * FROM {table[-1]}'

        return sql_string