from typing import Callable, Union, List
from typeguard import typechecked

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from splink.validate import (
    validate_settings_against_schema,
    validate_input_datasets,
    validate_link_type,
    validate_probabilities,
)
from splink.model import Model, load_model_from_json
from splink.case_statements import _check_jaro_registered
from splink.blocking import block_using_rules
from splink.gammas import add_gammas
from splink.iterate import iterate
from splink.expectation_step import run_expectation_step
from splink.term_frequencies import add_term_frequencies
from splink.vertically_concat import (
    vertically_concatenate_datasets,
)
from splink.break_lineage import default_break_lineage_blocked_comparisons

from splink.default_settings import normalise_probabilities


@typechecked
class Splink:
    def __init__(
        self,
        settings: dict,
        df_or_dfs: Union[DataFrame, List[DataFrame]],
        spark: SparkSession,
        save_state_fn: Callable = None,
        break_lineage_blocked_comparisons: Callable = default_break_lineage_blocked_comparisons,
    ):
        """Splink data linker

        Provides easy access to the core user-facing functionality of splink

        Args:
            settings (dict): splink settings dictionary
            df_or_dfs (Union[DataFrame, List[DataFrame]]): Either a single Spark dataframe to dedupe, or a list of
                Spark dataframes to link and or dedupe. Where `link_type` is `dedupe_only`, should be a single dataframe
                to dedupe. Where `link_type` is `link_only` or `link_and_dedupe`, show be a list of dfs.  Requires
                conformant dataframes (i.e. they must have same columns)
            spark (SparkSession): SparkSession object
            save_state_fn (function, optional):  A function provided by the user that takes one arguments, model (i.e.
                a Model from splink.model), and is executed each iteration.  This is a hook that allows the user to save
                the state between iterations, which is mostly useful for very large jobs which may need to be restarted
                from where they left off if they fail.
            break_lineage_blocked_comparisons (function, optional): Large jobs will likely run into memory errors
                unless the lineage is broken after blocking.  This is a user-provided function that takes one argument
                - df - and allows the user to break lineage.  For example, the function might save df to the AWS s3
                file system, and then reload it from the saved files.

        """

        self.spark = spark
        self.break_lineage_blocked_comparisons = break_lineage_blocked_comparisons

        _check_jaro_registered(spark)

        validate_settings_against_schema(settings)
        validate_link_type(df_or_dfs, settings)

        self.model = Model(settings, spark)
        self.settings_dict = self.model.current_settings_obj.settings_dict
        self.settings_dict = normalise_probabilities(self.settings_dict)
        validate_probabilities(self.settings_dict)
        # dfs is a list of dfs irrespective of whether input was a df or list of dfs
        if type(df_or_dfs) == DataFrame:
            dfs = [df_or_dfs]
        else:
            dfs = df_or_dfs

        self.df = vertically_concatenate_datasets(dfs)
        self.df = add_term_frequencies(self.df, self.model, self.spark)
        validate_input_datasets(self.df, self.model.current_settings_obj)
        self.save_state_fn = save_state_fn

    def manually_apply_fellegi_sunter_weights(self):
        """Compute match probabilities from m and u probabilities specified in the splink settings object

        Returns:
            DataFrame: A spark dataframe including a match probability column
        """
        df_comparison = block_using_rules(self.settings_dict, self.df, self.spark)
        df_gammas = add_gammas(df_comparison, self.settings_dict, self.spark)
        # see https://github.com/moj-analytical-services/splink/issues/187
        df_gammas = self.break_lineage_blocked_comparisons(df_gammas, self.spark)
        return run_expectation_step(df_gammas, self.model, self.spark)

    def get_scored_comparisons(self):
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
            save_state_fn=self.save_state_fn,
        )

        # In case the user's break lineage function has persisted it
        df_gammas.unpersist()

        return df_e

    def save_model_as_json(self, path: str, overwrite=False):
        """Save model (settings, parameters and parameter history) as a json file so it can later be re-loaded
        using load_from_json

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
        df (DataFrame, optional): The dataframe to dedupe. Where `link_type` is `dedupe_only`, the dataframe to dedupe.
            Should be ommitted `link_type` is `link_only` or `link_and_dedupe`.
        save_state_fn (function, optional):  A function provided by the user that takes one argument, params, and is
            executed each iteration.  This is a hook that allows the user to save the state between iterations, which
            is mostly useful for very large jobs which may need to be restarted from where they left off if they fail.

    Returns:
        splink.model.Model
    """
    model = load_model_from_json(path)

    linker = Splink(
        model.current_settings_obj.settings_dict, df_or_dfs, spark, save_state_fn
    )
    linker.model = model
    return linker
