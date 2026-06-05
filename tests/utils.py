from splink.internals.splink_dataframe import SplinkDataFrame


def assert_number_of_rows_with_gamma_value(
    df_pred: SplinkDataFrame,
    gamma_col_name: str,
    gamma_value: int,
    expected_number_of_rows: int,
):
    num_gamma_values_sdf = df_pred.query_sql(
        f"""
        SELECT
            COUNT(*) AS count
        FROM
            {{this}}
        WHERE
            {gamma_col_name} = {gamma_value}
        """
    )
    actual_number_of_rows = num_gamma_values_sdf.as_dict()["count"][0]
    assert_failure_string = (
        f"Gamma level '{gamma_value}' expected {expected_number_of_rows} rows, "
        f"but found {actual_number_of_rows}"
    )
    assert actual_number_of_rows == expected_number_of_rows, assert_failure_string


def assert_id_pair_has_gamma_value(
    df_pred: SplinkDataFrame,
    gamma_col_name: str,
    expected_gamma_level: int,
    id_pair: tuple[int, int],
):
    num_gamma_values_sdf = df_pred.query_sql(
        f"""
        SELECT
            {gamma_col_name} AS gamma_level
        FROM
            {{this}}
        WHERE
            unique_id_l = {id_pair[0]}
        AND
            unique_id_r = {id_pair[1]}
        """
    )
    actual_gamma_level = num_gamma_values_sdf.as_dict()["gamma_level"][0]
    assert_failure_string = (
        f"ID pair {id_pair} expected gamma level '{expected_gamma_level}', "
        f"but found level '{actual_gamma_level}'"
    )
    assert actual_gamma_level == expected_gamma_level, assert_failure_string
