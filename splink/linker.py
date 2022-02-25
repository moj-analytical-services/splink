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
from .term_frequencies import term_frequencies

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
    def __init__(self, settings_dict, input_tables, tf_tables={}):
        self.settings_dict = settings_dict

        self.settings_obj = Settings(settings_dict)

        # self.named_cache = {name: DataFrame(name, value)}
        self.named_cache = {}

        # self.hashed_cache = {hash: DataFrame(name, value)}
        self.hashed_cache = {}

        self.pipeline = SQLPipeline()

        self.input_dfs = self._get_input_dataframe_dict(input_tables)
        self.input_tf_tables = self._get_input_tf_dict(tf_tables)
        self._validate_input_dfs()
        self.em_training_sessions = []

        sql = vertically_concatente(self.input_dfs)
        self.enqueue_sql(sql, "__splink__df_concat")

        sqls = term_frequencies(self.settings_obj, self.input_tf_tables)
        for sql in sqls:
            self.enqueue_sql(sql["sql"], sql["output_table_name"])

        self.execute_sql_pipeline(materialise_as_hash=False)

    def enqueue_sql(self, sql, output_table_name):
        self.pipeline.enqueue_sql(sql, output_table_name)

    def execute_sql_pipeline(self, input_dataframes=[], materialise_as_hash=True):
        sql_gen = self.pipeline._generate_pipeline(input_dataframes)

        output_tablename_templated = self.pipeline.queue[-1].output_table_name

        dataframe = self.sql_to_dataframe(
            sql_gen, output_tablename_templated, materialise_as_hash
        )
        return dataframe

    def sql_to_dataframe(
        self, sql, output_tablename_templated, materialise_as_hash=True
    ):

        self.pipeline.reset()

        if output_tablename_templated in self.named_cache:
            print(f"Returning named cache {output_tablename_templated}")
            return self.named_cache[output_tablename_templated]

        hash = hashlib.sha256(sql.encode()).hexdigest()[:7]
        # Ensure hash is valid sql table name
        hash = "a_" + hash
        if hash in self.hashed_cache:
            print(f"Returning hashed cache {hash}")
            return self.hashed_cache[hash]

        print(f"Executing sql with hashed value {hash}")

        if materialise_as_hash:
            dataframe = self.execute_sql(sql, output_tablename_templated, hash)
            self.hashed_cache[hash] = dataframe
        else:
            dataframe = self.execute_sql(
                sql, output_tablename_templated, output_tablename_templated
            )
            self.named_cache[output_tablename_templated] = dataframe

        return dataframe

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

    def execute_sql(self, sql, templated_name, physical_name, transpile=True):
        raise NotImplementedError(f"execute_sql not implemented for {type(self)}")

    def _validate_input_dfs(self):
        for df in self.input_dfs.values():
            df.validate()

    def deterministic_link(self, return_df_as_value=True):

        df_dict = block_using_rules(self.settings_obj, self.input_dfs, self.execute_sql)
        if return_df_as_value:
            return df_dict["__splink__df_blocked"].df_value
        else:
            return df_dict

    def train_u_using_random_sampling(self, target_rows):

        estimate_u_values(self, target_rows)
        self.populate_m_u_from_trained_values()

    def train_m_from_label_column(self, label_colname):

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

        em_training_session = EMTrainingSession(
            self,
            blocking_rule,
            fix_u_probabilities=fix_u_probabilities,
            fix_m_probabilities=fix_m_probabilities,
            fix_proportion_of_matches=fix_proportion_of_matches,
            comparisons_to_deactivate=comparisons_to_deactivate,
            comparison_levels_to_reverse_blocking_rule=comparison_levels_to_reverse_blocking_rule,
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

    def _comparison_vectors(self):
        sql = block_using_rules(self.settings_obj)
        self.enqueue_sql(sql, "__splink__df_blocked")

        sql = compute_comparison_vector_values(self.settings_obj)
        self.enqueue_sql(sql, "__splink__df_comparison_vectors")
        return self.execute_sql_pipeline([])

    def predict(self):

        sql = block_using_rules(self.settings_obj)
        self.enqueue_sql(sql, "__splink__df_blocked")

        sql = compute_comparison_vector_values(self.settings_obj)
        self.enqueue_sql(sql, "__splink__df_comparison_vectors")

        sqls = predict(self.settings_obj)
        for sql in sqls:
            self.enqueue_sql(sql["sql"], sql["output_table_name"])

        predictions = self.execute_sql_pipeline([])
        return predictions
