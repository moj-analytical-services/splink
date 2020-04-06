from typing import Callable

try:
    from pyspark.sql.dataframe import DataFrame
    from pyspark.sql.session import SparkSession
except ImportError:
    DataFrame = None
    SparkSession = None

from splink.settings import complete_settings_dict
from splink.validate import validate_settings
from splink.params import Params, load_params_from_json
from splink.case_statements import _check_jaro_registered
from splink.blocking import block_using_rules
from splink.gammas import add_gammas
from splink.iterate import iterate
from splink.expectation_step import run_expectation_step
from splink.term_frequencies import make_adjustment_for_term_frequencies
from splink.check_types import check_types
from splink.break_lineage import default_break_lineage_blocked_comparisons, default_break_lineage_scored_comparisons

# For type hints. I use try except to ensure that the sql generation functions work even if spark does not exist
try:
    from pyspark.sql.dataframe import DataFrame
    from pyspark.sql.session import SparkSession

    spark_exists = True
except ImportError:
    DataFrame = None
    SparkSession = None
    spark_exists = False


class Splink:
    @check_types
    def __init__(
        self,
        settings: dict,
        spark: SparkSession,
        df_l: DataFrame = None,
        df_r: DataFrame = None,
        df: DataFrame = None,
        save_state_fn: Callable = None,
        break_lineage_blocked_comparisons: Callable = default_break_lineage_blocked_comparisons, 
        break_lineage_scored_comparisons: Callable = default_break_lineage_scored_comparisons
    ):
        """splink data linker

        Provides easy access to the core user-facing functinoality of splink

        Args:
            settings (dict): splink settings dictionary
            spark (SparkSession): SparkSession object
            df_l (DataFrame, optional): A dataframe to link/dedupe. Where `link_type` is `link_only` or `link_and_dedupe`, one of the two dataframes to link. Should be ommitted `link_type` is `dedupe_only`.
            df_r (DataFrame, optional): A dataframe to link/dedupe. Where `link_type` is `link_only` or `link_and_dedupe`, one of the two dataframes to link. Should be ommitted `link_type` is `dedupe_only`.
            df (DataFrame, optional): The dataframe to dedupe. Where `link_type` is `dedupe_only`, the dataframe to dedupe. Should be ommitted `link_type` is `link_only` or `link_and_dedupe`.
            save_state_fn (function, optional):  A function provided by the user that takes two arguments, params and settings, and is executed each iteration.  This is a hook that allows the user to save the state between iterations, which is mostly useful for very large jobs which may need to be restarted from where they left off if they fail.
            break_lineage_blocked_comparisons (function, optional): Large jobs will likely run into memory errors unless the lineage is broken after blocking.  This is a user-provided function that takes one argument - df - and allows the user to break lineage.  For example, the function might save df to the AWS s3 file system, and then reload it from the saved files.
            break_lineage_scored_comparisons (function, optional): Large jobs will likely run into memory errors unless the lineage is broken after comparisons are scored and before term frequency adjustments.  This is a user-provided function that takes one argument - df - and allows the user to break lineage.  For example, the function might save df to the AWS s3 file system, and then reload it from the saved files.
        """

        self.spark = spark
        self.break_lineage_blocked_comparisons = break_lineage_blocked_comparisons
        self.break_lineage_scored_comparisons = break_lineage_scored_comparisons
        _check_jaro_registered(spark)

        settings = complete_settings_dict(settings, spark)
        validate_settings(settings)
        self.settings = settings

        self.params = Params(settings, spark)

        self.df_r = df_r
        self.df_l = df_l
        self.df = df
        self.save_state_fn = save_state_fn
        self._check_args()

    def _check_args(self):

        link_type = self.settings["link_type"]

        if link_type == "dedupe_only":
            check_1 = self.df_r is None
            check_2 = self.df_l is None
            check_3 = isinstance(self.df, DataFrame)

            if not all([check_1, check_2, check_3]):
                raise ValueError(
                    "For link_type = 'dedupe_only', you must pass a single Spark dataframe to Splink using the df argument. "
                    "The df_l and df_r arguments should be omitted or set to None. "
                    "e.g. linker = Splink(settings, spark, df=my_df)"
                )

        if link_type in ["link_only", "link_and_dedupe"]:
            check_1 = isinstance(self.df_l, DataFrame)
            check_2 = isinstance(self.df_r, DataFrame)
            check_3 = self.df is None

            if not all([check_1, check_2, check_3]):
                raise ValueError(
                    f"For link_type = '{link_type}', you must pass two Spark dataframes to Splink using the df_l and df_r argument. "
                    "The df argument should be omitted or set to None. "
                    "e.g. linker = Splink(settings, spark, df_l=my_first_df, df_r=df_to_link_to_first_one)"
                )

    def _get_df_comparison(self):

        if self.settings["link_type"] == "dedupe_only":
            return block_using_rules(self.settings, self.spark, df=self.df)

        if self.settings["link_type"] in ("link_only", "link_and_dedupe"):
            return block_using_rules(
                self.settings, self.spark, df_l=self.df_l, df_r=self.df_r
            )

    def manually_apply_fellegi_sunter_weights(self):
        """Compute match probabilities from m and u probabilities specified in the splink settings object

        Returns:
            DataFrame: A spark dataframe including a match probability column
        """
        df_comparison = self._get_df_comparison()
        df_gammas = add_gammas(df_comparison, self.settings, self.spark)
        return run_expectation_step(df_gammas, self.params, self.settings, self.spark)

    def get_scored_comparisons(self):
        """Use the EM algorithm to estimate model parameters and return match probabilities.

        Note: Does not compute term frequency adjustments.

        Returns:
            DataFrame: A spark dataframe including a match probability column
        """

        df_comparison = self._get_df_comparison()

        df_gammas = add_gammas(df_comparison, self.settings, self.spark)

        df_gammas = self.break_lineage_blocked_comparisons(df_gammas, self.spark)

        df_e = iterate(
            df_gammas,
            self.params,
            self.settings,
            self.spark,
            compute_ll=False,
            save_state_fn=self.save_state_fn,
        )
        
        # In case the user's break lineage function has persisted it
        df_gammas.unpersist()

        df_e = self.break_lineage_scored_comparisons(df_e, self.spark)

        df_e_adj = self._make_term_frequency_adjustments(df_e)

        df_e.unpersist()

        return df_e_adj

    def _make_term_frequency_adjustments(self, df_e: DataFrame):
        """Take the outputs of 'get_scored_comparisons' and make term frequency adjustments on designated columns in the settings dictionary

        Args:
            df_e (DataFrame): A dataframe produced by the get_scored_comparisons method

        Returns:
            DataFrame: A spark dataframe including a column with term frequency adjusted match probabilities
        """

        return make_adjustment_for_term_frequencies(
            df_e,
            self.params,
            self.settings,
            retain_adjustment_columns=True,
            spark=self.spark,
        )

    def save_model_as_json(self, path:str, overwrite=False):
        """Save model (settings, parameters and parameter history) as a json file so it can later be re-loaded using load_from_json

        Args:
            path (str): Path to the json file.
            overwrite (bool): Whether to overwrite the file if it exsits
        """
        self.params.save_params_to_json_file(path, overwrite=overwrite)




def load_from_json(path: str,
        spark: SparkSession,
        df_l: DataFrame = None,
        df_r: DataFrame = None,
        df: DataFrame = None,
        save_state_fn: Callable = None):
    """Load a splink model from a json file which has previously been created using 'save_model_as_json'

    Args:
        path (string): path to json file created using Splink.save_model_as_json
        spark (SparkSession): SparkSession object
        df_l (DataFrame, optional): A dataframe to link/dedupe. Where `link_type` is `link_only` or `link_and_dedupe`, one of the two dataframes to link. Should be ommitted `link_type` is `dedupe_only`.
        df_r (DataFrame, optional): A dataframe to link/dedupe. Where `link_type` is `link_only` or `link_and_dedupe`, one of the two dataframes to link. Should be ommitted `link_type` is `dedupe_only`.
        df (DataFrame, optional): The dataframe to dedupe. Where `link_type` is `dedupe_only`, the dataframe to dedupe. Should be ommitted `link_type` is `link_only` or `link_and_dedupe`.
        save_state_fn (function, optional):  A function provided by the user that takes two arguments, params and settings, and is executed each iteration.  This is a hook that allows the user to save the state between iterations, which is mostly useful for very large jobs which may need to be restarted from where they left off if they fail.
    """
    params = load_params_from_json(path)
    settings = params.settings
    linker = Splink(settings, spark, df_l, df_r, df, save_state_fn)
    linker.params = params
    return linker