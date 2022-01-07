from collections import OrderedDict
import logging


from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from typeguard import typechecked
from .logging_utils import _format_sql
from .settings import ComparisonColumn
from .default_settings import complete_settings_dict
from .ordered_set import OrderedSet
from .parse_case_statement import generate_sql_from_parsed_case_expr

logger = logging.getLogger(__name__)


def _add_left_right(columns_to_retain, name):
    columns_to_retain.add(name + "_l")
    columns_to_retain.add(name + "_r")
    return columns_to_retain


def _add_unique_id_and_source_dataset(
    cols_set: OrderedSet, uid_name: str, sd_name: str, retain_source_dataset_col: bool
):
    if retain_source_dataset_col:
        cols_set.add(f"{sd_name}_l")
        cols_set.add(f"{uid_name}_l")
        cols_set.add(f"{sd_name}_r")
        cols_set.add(f"{uid_name}_r")
    else:
        cols_set.add(f"{uid_name}_l")
        cols_set.add(f"{uid_name}_r")
    return cols_set


def _get_select_expression_gammas(
    settings: dict, retain_source_dataset_col: bool, retain_tf_cols: bool
):
    """Get a select expression which picks which columns to keep in df_gammas

    Args:
        settings (dict): A `splink` settings dictionary
        retain_source_dataset: whether to retain the source dataset columns
        retain_tf_cols: whether to retain the individual term frequency columns

    Returns:
        str: A select expression
    """

    # Use ordered dict as an ordered set - i.e. to make sure we don't have duplicate cols to retain

    select_columns = OrderedSet()

    uid = settings["unique_id_column_name"]
    sds = settings["source_dataset_column_name"]
    select_columns = _add_unique_id_and_source_dataset(
        select_columns, uid, sds, retain_source_dataset_col
    )

    for col in settings["comparison_columns"]:
        cc = ComparisonColumn(col)
        if settings["retain_matching_columns"]:
            for col_name in cc.columns_used:
                select_columns = _add_left_right(select_columns, col_name)
        if col["term_frequency_adjustments"]:
            if retain_tf_cols:
                select_columns = _add_left_right(select_columns, f"tf_{cc.name}")
        case_expr = generate_sql_from_parsed_case_expr(
            col["comparison_levels"], cc.name
        )
        select_columns.add(case_expr)

    for c in settings["additional_columns_to_retain"]:
        select_columns = _add_left_right(select_columns, c)

    if "blocking_rules" in settings:
        if len(settings["blocking_rules"]) > 0:
            select_columns.add("match_key")

    return ", ".join(select_columns)


def _retain_source_dataset_column(settings_dict, df):
    # Want to retain source dataset column in all cases
    # except when link type is dedupe only
    # and the column does not exist in the data
    if settings_dict["link_type"] != "dedupe_only":
        return True

    source_dataset_colname = settings_dict.get(
        "source_dataset_column_name", "source_dataset"
    )
    if f"{source_dataset_colname}_l" in df.columns:
        return True
    else:
        return False


def _retain_tf_columns(settings_dict, df):
    # If all necessary TF columns are in the data,
    # make sure they are retained
    tf_cols = [
        f"tf_{cc['col_name']}" if "col_name" in cc else f"tf_{cc['custom_name']}"
        for cc in settings_dict["comparison_columns"]
        if cc["term_frequency_adjustments"]
    ]

    cols = OrderedSet()
    [_add_left_right(cols, c) for c in tf_cols]

    return all([col in df.columns for col in cols])


def _sql_gen_add_gammas(
    settings: dict,
    df_comparison: DataFrame,
    table_name: str = "df_comparison",
):
    """Build SQL statement that adds gamma columns to the comparison dataframe

    Args:
        settings (dict): `splink` settings dict
        unique_id_col (str, optional): Name of the unique id column. Defaults to "unique_id".
        table_name (str, optional): Name of the comparison df. Defaults to "df_comparison".

    Returns:
        str: A SQL string
    """

    retain_source_dataset = _retain_source_dataset_column(settings, df_comparison)
    retain_tf_cols = _retain_tf_columns(settings, df_comparison)
    select_cols_expr = _get_select_expression_gammas(
        settings, retain_source_dataset, retain_tf_cols
    )

    sql = f"""
    select {select_cols_expr}
    from {table_name}
    """

    return sql


@typechecked
def add_gammas(
    df_comparison: DataFrame,
    settings_dict: dict,
    spark: SparkSession,
    unique_id_col: str = "unique_id",
):
    """Compute the comparison vectors and add them to the dataframe.  See
    https://imai.fas.harvard.edu/research/files/linkage.pdf for more details of what is meant by comparison vectors

    Args:
        df_comparison (spark dataframe): A Spark dataframe containing record comparisons, with records compared using the convention col_name_l, col_name_r
        settings_dict (dict): The `splink` settings dictionary
        spark (Spark session): The Spark session object

    Returns:
        Spark dataframe: A dataframe containing new columns representing the gammas of the model
    """

    settings_dict = complete_settings_dict(settings_dict, spark)

    sql = _sql_gen_add_gammas(settings_dict, df_comparison)

    logger.debug(_format_sql(sql))
    df_comparison.createOrReplaceTempView("df_comparison")
    df_gammas = spark.sql(sql)

    return df_gammas
