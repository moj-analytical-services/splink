# For type hints. Try except to ensure the sql_gen functions even if spark doesn't exist.
try:
    from pyspark.sql.dataframe import DataFrame
    from pyspark.sql.session import SparkSession
except ImportError:
    DataFrame = None
    SparkSession = None

from sparklink.settings import complete_settings_dict
from sparklink.validate import validate_settings
from sparklink.params import Params
from sparklink.case_statements import _check_jaro_registered
from sparklink.blocking import block_using_rules
from sparklink.gammas import add_gammas
from sparklink.iterate import iterate
from sparklink.expectation_step import run_expectation_step
from sparklink.term_frequencies import make_adjustment_for_term_frequencies
from sparklink.check_types import check_types

try:
    from pyspark.sql.dataframe import DataFrame
    from pyspark.sql.session import SparkSession

    spark_exists = True
except ImportError:
    DataFrame = None
    SparkSession = None
    spark_exists = False


class Sparklink:
    @check_types
    def __init__(
        self,
        settings: dict,
        spark: SparkSession,
        df_l: DataFrame = None,
        df_r: DataFrame = None,
        df: DataFrame = None,
    ):

        self.spark = spark
        _check_jaro_registered(spark)

        settings = complete_settings_dict(settings)
        validate_settings(settings)
        self.settings = settings

        self.params = Params(settings)

        self.df_r = df_r
        self.df_l = df_l
        self.df = df
        self._check_args()

    def _check_args(self):

        link_type = self.settings["link_type"]

        if link_type == "dedupe_only":
            check_1 = self.df_r is None
            check_2 = self.df_l is None
            check_3 = isinstance(self.df, DataFrame)

            if not all([check_1, check_2, check_3]):
                raise ValueError(
                    "For link_type = 'dedupe_only', you must pass a single Spark dataframe to Sparklink using the df argument. "
                    "The df_l and df_r arguments should be omitted or set to None. "
                    "e.g. linker = Sparklink(settings, spark, df=my_df)"
                )

        if link_type in ["link_only", "link_and_dedupe"]:
            check_1 = self.df_r is isinstance(self.df, DataFrame)
            check_2 = self.df_l is isinstance(self.df, DataFrame)
            check_3 = self.df is None

            if not all([check_1, check_2, check_3]):
                raise ValueError(
                    f"For link_type = '{link_type}', you must pass two Spark dataframes to Sparklink using the df_l and df_r argument. "
                    "The df argument should be omitted or set to None. "
                    "e.g. linker = Sparklink(settings, spark, df_l=my_first_df, df_r=df_to_link_to_first_one)"
                )

    def _get_df_comparison(self):

        if self.settings["link_type"] == "dedupe_only":
            return block_using_rules(self.settings, self.spark, df=self.df)

        if self.settings["link_type"] in ("link_only", "link_and_dedupe"):
            return block_using_rules(
                self.settings, self.spark, df_l=self.df_l, df_r=self.df_r
            )

    def manually_apply_fellegi_sunter_weights(self):
        df_comparison = self._get_df_comparison()
        df_gammas = add_gammas(df_comparison, self.settings, self.spark)
        return run_expectation_step(df_gammas, self.params, self.settings, self.spark)

    def get_scored_comparisons(self, num_iterations=20):

        df_comparison = self._get_df_comparison()

        df_gammas = add_gammas(df_comparison, self.settings, self.spark)

        df_gammas.persist()

        df_e = iterate(
            df_gammas,
            self.params,
            self.settings,
            self.spark,
            log_iteration=True,
            num_iterations=num_iterations,
            compute_ll=False,
        )
        df_gammas.unpersist()
        return df_e

    def make_term_frequency_adjustments(self, df_e:DataFrame):

        return make_adjustment_for_term_frequencies(
            df_e,
            self.params,
            self.settings,
            retain_adjustment_columns=True,
            spark=self.spark,
        )

