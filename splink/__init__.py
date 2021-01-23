from typing import Callable, Union, List
from typeguard import typechecked

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from splink.validate import (
    validate_settings,
    validate_input_datasets,
    validate_link_type,
)
from splink.model import Model, load_model_from_json
from splink.case_statements import _check_jaro_registered
from splink.blocking import block_using_rules
from splink.gammas import add_gammas
from splink.iterate import iterate
from splink.expectation_step import run_expectation_step
from splink.term_frequencies import make_adjustment_for_term_frequencies
from splink.vertically_concat import (
    vertically_concatenate_datasets,
)
from splink.break_lineage import (
    default_break_lineage_blocked_comparisons,
    default_break_lineage_scored_comparisons,
)


@typechecked
class Splink:
    def __init__(
        self,
        settings: dict,
        df_or_dfs: Union[DataFrame, List[DataFrame]],
        spark: SparkSession,
        save_state_fn: Callable = None,
        break_lineage_blocked_comparisons: Callable = default_break_lineage_blocked_comparisons,
        break_lineage_scored_comparisons: Callable = default_break_lineage_scored_comparisons,
    ):
        """Splink data linker

        Provides easy access to the core user-facing functinoality of splink

        Args:
            settings (dict): splink settings dictionary
            df_or_dfs (Union[DataFrame, List[DataFrame]]): Either a single Spark dataframe to dedupe, or a list of Spark dataframe to link and or dedupe. Where `link_type` is `dedupe_only`, should be a single dataframe to dedupe. Where `link_type` is `link_only` or `link_and_dedupe`, show be a list of dfs.  Requires conformant dataframes (i.e. they must have same columns)
            spark (SparkSession): SparkSession object
            save_state_fn (function, optional):  A function provided by the user that takes one arguments, model (i.e. a Model from splink.model), and is executed each iteration.  This is a hook that allows the user to save the state between iterations, which is mostly useful for very large jobs which may need to be restarted from where they left off if they fail.
            break_lineage_blocked_comparisons (function, optional): Large jobs will likely run into memory errors unless the lineage is broken after blocking.  This is a user-provided function that takes one argument - df - and allows the user to break lineage.  For example, the function might save df to the AWS s3 file system, and then reload it from the saved files.
            break_lineage_scored_comparisons (function, optional): Large jobs will likely run into memory errors unless the lineage is broken after comparisons are scored and before term frequency adjustments.  This is a user-provided function that takes one argument - df - and allows the user to break lineage.  For example, the function might save df to the AWS s3 file system, and then reload it from the saved files.
        """

        self.spark = spark
        self.break_lineage_blocked_comparisons = break_lineage_blocked_comparisons
        self.break_lineage_scored_comparisons = break_lineage_scored_comparisons
        _check_jaro_registered(spark)

        validate_settings(settings)
        validate_link_type(df_or_dfs, settings)
        self.model = Model(settings, spark)
        self.settings_dict = self.model.current_settings_obj.settings_dict

        # dfs is a list of dfs irrespective of whether input was a df or list of dfs
        if type(df_or_dfs) == DataFrame:
            dfs = [df_or_dfs]
        else:
            dfs = df_or_dfs

        self.df = vertically_concatenate_datasets(dfs)
        validate_input_datasets(self.df, self.model.current_settings_obj)
        self.save_state_fn = save_state_fn

    def manually_apply_fellegi_sunter_weights(self):
        """Compute match probabilities from m and u probabilities specified in the splink settings object

        Returns:
            DataFrame: A spark dataframe including a match probability column
        """
        df_comparison = block_using_rules(self.settings_dict, self.df, self.spark)
        df_gammas = add_gammas(df_comparison, self.settings_dict, self.spark)
        return run_expectation_step(df_gammas, self.model, self.spark)

    def get_scored_comparisons(self, compute_ll=False):
        """Use the EM algorithm to estimate model parameters and return match probabilities.

        Returns:
            DataFrame: A spark dataframe including a match probability column
        """

        df_comparison = block_using_rules(self.settings_dict, self.df, self.spark)

        df_gammas = add_gammas(df_comparison, self.settings_dict, self.spark)

        df_gammas = self.break_lineage_blocked_comparisons(df_gammas, self.spark)

        df_e = iterate(
            df_gammas,
            self.model,
            self.spark,
            compute_ll=compute_ll,
            save_state_fn=self.save_state_fn,
        )

        # In case the user's break lineage function has persisted it
        df_gammas.unpersist()

        df_e = self.break_lineage_scored_comparisons(df_e, self.spark)

        df_e_adj = self.make_term_frequency_adjustments(df_e)

        df_e.unpersist()

        return df_e_adj

    def make_term_frequency_adjustments(self, df_e: DataFrame):
        """Take the outputs of 'get_scored_comparisons' and make term frequency adjustments on designated columns in the settings dictionary

        Args:
            df_e (DataFrame): A dataframe produced by the get_scored_comparisons method

        Returns:
            DataFrame: A spark dataframe including a column with term frequency adjusted match probabilities
        """

        return make_adjustment_for_term_frequencies(
            df_e,
            self.model,
            retain_adjustment_columns=True,
            spark=self.spark,
        )

    def save_model_as_json(self, path: str, overwrite=False):
        """Save model (settings, parameters and parameter history) as a json file so it can later be re-loaded using load_from_json

        Args:
            path (str): Path to the json file.
            overwrite (bool): Whether to overwrite the file if it exsits
        """
        self.model.save_model_to_json_file(path, overwrite=overwrite)


@typechecked
def load_from_json(
    path: str,
    df_or_dfs: Union[DataFrame, List[DataFrame]],
    spark: SparkSession,
    save_state_fn: Callable = None,
):
    """Load a splink model from a json file which has previously been created using 'save_model_as_json'

    Args:
        path (string): path to json file created using Splink.save_model_as_json
        spark (SparkSession): SparkSession object
        df (DataFrame, optional): The dataframe to dedupe. Where `link_type` is `dedupe_only`, the dataframe to dedupe. Should be ommitted `link_type` is `link_only` or `link_and_dedupe`.
        save_state_fn (function, optional):  A function provided by the user that takes one argument, params, and is executed each iteration.  This is a hook that allows the user to save the state between iterations, which is mostly useful for very large jobs which may need to be restarted from where they left off if they fail.

    Returns:
        splink.model.Model
    """
    model = load_model_from_json(path)

    linker = Splink(
        model.current_settings_obj.settings_dict, df_or_dfs, spark, save_state_fn
    )
    linker.model = model
    return linker
