import warnings


def _check_jaro_registered(spark):

    if spark is None:
        return False

    if (
        spark == "supress_warnings"
    ):  # Allows us to surpress the warning in the test suite
        return False

    for fn in spark.catalog.listFunctions():
        if fn.name == "jaro_winkler_sim":
            return True

    warnings.warn(
        "Custom string comparison functions such as jaro_winkler_sim are available in Spark "
        "Or you did not pass 'spark' (the SparkSession) into 'Model' "
        "You can import these functions using the scala-udf-similarity-0.0.7.jar provided with Splink"
    )
    return False


def _add_as_gamma_to_case_statement(case_statement: str, gamma_col_name):
    """As the correct column alias to the case statement if it does not exist

    Args:
        case_statement (str): Original case statement

    Returns:
        str: case_statement with correct alias
    """
    if gamma_col_name is None:
        return case_statement

    sl = case_statement.lower()
    # What we're trying to do is distinguish between 'case when end as gamma_blah' from case when end
    sl = sl.replace("\n", " ").replace("\r", "")
    sl = sl.strip()
    # Only need to do anything if the last non-blank string is not ' end'
    if sl[-4:] != " end":
        # The case statement has a 'end as gamma blah'
        last_end_index = sl.rfind(" end ")
        sl = sl[: last_end_index + 5]
    return f"{sl} as gamma_{gamma_col_name}"


def _check_no_obvious_problem_with_case_statement(case_statement):
    seems_valid = True
    cs_l = case_statement.lower()
    if "case" not in cs_l:
        seems_valid = False
    if "end" not in cs_l:
        seems_valid = False
    if "when" not in cs_l:
        seems_valid = False
    if "then" not in cs_l:
        seems_valid = False

    if not seems_valid:
        s1 = "The case expression you provided does not seem to be valid SQL."
        s2 = f"Expression provided is: '{case_statement}'"
        raise ValueError(f"{s1} {s2}")


def sql_gen_case_smnt_strict_equality_2(col_name, gamma_col_name=None):
    c = f"""case
    when {col_name}_l is null or {col_name}_r is null then -1
    when {col_name}_l = {col_name}_r then 1
    else 0 end"""

    return _add_as_gamma_to_case_statement(c, gamma_col_name)


# String comparison select statements

# jaro winkler statements, using param values suggested on page 355 of
#  https://imai.fas.harvard.edu/research/files/linkage.pdf
# American Political Science Review (2019) 113, 2, 353–371, Using a Probabilistic Model to Assist Merging of Large-Scale
# Administrative Records


def sql_gen_case_stmt_jaro_2(col_name, threshold, gamma_col_name=None):
    c = f"""case
    when {col_name}_l is null or {col_name}_r is null then -1
    when jaro_winkler_sim({col_name}_l, {col_name}_r) >= {threshold} then 1
    else 0 end"""

    return _add_as_gamma_to_case_statement(c, gamma_col_name)


def sql_gen_case_stmt_jaro_3(
    col_name, gamma_col_name=None, threshold1=1.0, threshold2=0.88
):
    c = f"""case
    when {col_name}_l is null or {col_name}_r is null then -1
    when jaro_winkler_sim({col_name}_l, {col_name}_r) >= {threshold1} then 2
    when jaro_winkler_sim({col_name}_l, {col_name}_r) >= {threshold2} then 1
    else 0 end"""

    return _add_as_gamma_to_case_statement(c, gamma_col_name)


def sql_gen_case_stmt_jaro_4(
    col_name, gamma_col_name=None, threshold1=1.0, threshold2=0.88, threshold3=0.7
):
    c = f"""case
    when {col_name}_l is null or {col_name}_r is null then -1
    when jaro_winkler_sim({col_name}_l, {col_name}_r) >= {threshold1} then 3
    when jaro_winkler_sim({col_name}_l, {col_name}_r) >= {threshold2} then 2
    when jaro_winkler_sim({col_name}_l, {col_name}_r) >= {threshold3} then 1
    else 0 end"""

    return _add_as_gamma_to_case_statement(c, gamma_col_name)


def sql_gen_case_stmt_levenshtein_rel_3(
    col_name, gamma_col_name=None, threshold1=0.0, threshold2=0.3
):
    c = f"""case
    when {col_name}_l is null or {col_name}_r is null then -1
    when levenshtein({col_name}_l, {col_name}_r)/((length({col_name}_l) + length({col_name}_r))/2) <= {threshold1} then 2
    when levenshtein({col_name}_l, {col_name}_r)/((length({col_name}_l) + length({col_name}_r))/2) <= {threshold2} then 1
    else 0 end"""

    return _add_as_gamma_to_case_statement(c, gamma_col_name)


def sql_gen_case_stmt_levenshtein_abs_3(
    col_name, gamma_col_name=None, threshold1=0, threshold2=2
):
    c = f"""case
    when {col_name}_l is null or {col_name}_r is null then -1
    when {col_name}_l = {col_name}_r then 2
    when levenshtein({col_name}_l, {col_name}_r) <= {threshold1} then 2
    when levenshtein({col_name}_l, {col_name}_r) <= {threshold2} then 1
    else 0 end"""

    return _add_as_gamma_to_case_statement(c, gamma_col_name)


def sql_gen_case_stmt_levenshtein_rel_4(
    col_name, gamma_col_name=None, threshold1=0.0, threshold2=0.2, threshold3=0.4
):
    c = f"""case
    when {col_name}_l is null or {col_name}_r is null then -1
    when {col_name}_l = {col_name}_r then 3
    when levenshtein({col_name}_l, {col_name}_r)/((length({col_name}_l) + length({col_name}_r))/2) <= {threshold1} then 3
    when levenshtein({col_name}_l, {col_name}_r)/((length({col_name}_l) + length({col_name}_r))/2) <= {threshold2}
    then 2
    when levenshtein({col_name}_l, {col_name}_r)/((length({col_name}_l) + length({col_name}_r))/2) <= {threshold3}
    then 1
    else 0 end"""

    return _add_as_gamma_to_case_statement(c, gamma_col_name)


def sql_gen_case_stmt_levenshtein_abs_4(
    col_name, gamma_col_name=None, threshold1=0, threshold2=1, threshold3=2
):
    c = f"""case
    when {col_name}_l is null or {col_name}_r is null then -1
    when {col_name}_l = {col_name}_r then 3
    when levenshtein({col_name}_l, {col_name}_r) <= {threshold1} then 3
    when levenshtein({col_name}_l, {col_name}_r) <= {threshold2} then 2
    when levenshtein({col_name}_l, {col_name}_r) <= {threshold3} then 1
    else 0 end"""

    return _add_as_gamma_to_case_statement(c, gamma_col_name)


# Numeric value statements


def _sql_gen_max_of_two_cols(col1, col2):
    return f"""
    case
    when {col1} > {col2} then {col1}
    else {col2}
    end
    """


def _sql_gen_abs_diff(col1, col2):
    return f"(abs({col1} - {col2}))"


def sql_gen_case_stmt_numeric_float_equality_2(col_name, gamma_col_name=None):

    col1 = f"{col_name}_l"
    col2 = f"{col_name}_r"

    abs_difference = _sql_gen_abs_diff(col1, col2)

    c = f"""case
    when {col_name}_l is null or {col_name}_r is null then -1
    when {abs_difference} < 0.00001 then 1
    else 0 end"""

    return _add_as_gamma_to_case_statement(c, gamma_col_name)


def sql_gen_case_stmt_numeric_abs_3(
    col_name, gamma_col_name=None, abs_amount=1, equality_threshold=0.0001
):

    col1 = f"{col_name}_l"
    col2 = f"{col_name}_r"

    abs_difference = _sql_gen_abs_diff(col1, col2)

    c = f"""case
    when {col_name}_l is null or {col_name}_r is null then -1
    when {abs_difference} < {equality_threshold} THEN 2
    when {abs_difference} < {abs_amount} THEN 1
    else 0 end"""

    return _add_as_gamma_to_case_statement(c, gamma_col_name)


def sql_gen_case_stmt_numeric_abs_4(
    col_name,
    gamma_col_name=None,
    abs_amount_low=1,
    abs_amount_high=10,
    equality_threshold=0.0001,
):

    col1 = f"{col_name}_l"
    col2 = f"{col_name}_r"

    abs_difference = _sql_gen_abs_diff(col1, col2)

    c = f"""case
    when {col_name}_l is null or {col_name}_r is null then -1
    when {abs_difference} < {equality_threshold} THEN 3
    when {abs_difference} < {abs_amount_low} THEN 2
    when {abs_difference} < {abs_amount_high} THEN 1
    else 0 end"""

    return _add_as_gamma_to_case_statement(c, gamma_col_name)


def sql_gen_case_stmt_numeric_perc_3(
    col_name, gamma_col_name=None, per_diff=0.05, equality_threshold=0.0001
):

    col1 = f"{col_name}_l"
    col2 = f"{col_name}_r"

    max_of_cols = _sql_gen_max_of_two_cols(col1, col2)
    abs_difference = _sql_gen_abs_diff(col1, col2)

    c = f"""case
    when {col_name}_l is null or {col_name}_r is null then -1
    when {abs_difference}/abs({max_of_cols}) < {equality_threshold} then 2
    when {abs_difference}/abs({max_of_cols}) < {per_diff} then 1
    else 0 end"""

    return _add_as_gamma_to_case_statement(c, gamma_col_name)


def sql_gen_case_stmt_numeric_perc_4(
    col_name,
    gamma_col_name=None,
    per_diff_low=0.05,
    per_diff_high=0.10,
    equality_threshold=0.0001,
):

    col1 = f"{col_name}_l"
    col2 = f"{col_name}_r"

    max_of_cols = _sql_gen_max_of_two_cols(col1, col2)
    abs_difference = _sql_gen_abs_diff(col1, col2)

    c = f"""case
    when {col_name}_l is null or {col_name}_r is null then -1
    when {abs_difference}/abs({max_of_cols}) < {equality_threshold} then 3
    when {abs_difference}/abs({max_of_cols}) < {per_diff_low} then 2
    when {abs_difference}/abs({max_of_cols}) < {per_diff_high} then 1
    else 0 end"""

    return _add_as_gamma_to_case_statement(c, gamma_col_name)


def _sql_gen_get_or_list_jaro(col_name, other_name_cols, threshold=1.0):
    # Note the ifnull 1234 just ensures that if one of the other columns is null,
    # the jaro score is lower than the threshold
    ors = [
        f"jaro_winkler_sim(ifnull({col_name}_l, '1234abcd5678'), ifnull({n}_r, '987pqrxyz654')) >= {threshold}"
        for n in other_name_cols
    ]
    ors_string = " OR ".join(ors)
    return f"({ors_string})"


def sql_gen_case_stmt_name_inversion_4(
    col_name: str,
    other_name_cols: list,
    gamma_col_name=None,
    threshold1=1.0,
    threshold2=0.88,
    include_dmeta=False,
):
    """Generate a case expression which can handle name inversions where e.g. surname and forename are inverted

    Args:
        col_name (str): The name of the column we want to generate a custom case expression for e.g. surname
        other_name_cols (list): The name of the other columns that contain names e.g. forename1, forename2
        gamma_col_name (str, optional): . The name of the column, for the alias e.g. surname
        threshold1 (float, optional): Jaro threshold for almost exact match. Defaults to 1.0.
        threshold2 (float, optional): Jaro threshold for close match Defaults to 0.88.
        include_dmeta (bool, optional): Also allow a dmetaphone match at threshold2

    Returns:
        str: A sql string
    """

    dmeta_statment = ""
    if include_dmeta:
        dmeta_statment = f"""
        when Dmetaphone({col_name}_l) = Dmetaphone({col_name}_r) then 1
        when DmetaphoneAlt({col_name}_l) = DmetaphoneAlt({col_name}_r) then 1
        """

    c = f"""case
    when {col_name}_l is null or {col_name}_r is null then -1
    when jaro_winkler_sim({col_name}_l, {col_name}_r) >= {threshold1} then 3
    when {_sql_gen_get_or_list_jaro(col_name, other_name_cols, threshold1)} then 2
    {dmeta_statment}
    when jaro_winkler_sim({col_name}_l, {col_name}_r) >= {threshold2} then 1
    else 0 end"""

    return _add_as_gamma_to_case_statement(c, gamma_col_name)


# Array comparison functions


def sql_gen_case_stmt_array_intersect_2(
    col_name: str, gamma_col_name=None, zero_length_is_null=True
):
    """Generate a case comparison which is 1 if the size of the intersection of the arrays is
    one or more.  Otherwise zero.


    Args:
        col_name (str): The name of the column we want to generate a custom case expression for e.g. phone_number
        gamma_col_name (str, optional): . The name of the column, for the alias e.g. surname
        zero_length_is_null (bool, optional):  Whether to treat a zero length array as a null. Defaults to True.
    """

    zero_length_expr = ""
    if zero_length_is_null:
        zero_length_expr = (
            f"when size({col_name}_l) = 0 or size({col_name}_r) = 0 then -1"
        )

    c = f"""case
    when {col_name}_l is null or {col_name}_r is null then -1
    {zero_length_expr}
    when size(array_intersect({col_name}_l, {col_name}_r)) >= 1 then 1
    else 0
    end
    """

    return _add_as_gamma_to_case_statement(c, gamma_col_name)


def sql_gen_case_stmt_array_intersect_3(
    col_name: str, gamma_col_name=None, zero_length_is_null=True
):
    """Generate a three level case comparison based on the size of the intersection of the arrays

    Args:
        col_name (str): The name of the column we want to generate a custom case expression for e.g. phone_number
        gamma_col_name (str, optional): . The name of the column, for the alias e.g. surname
        zero_length_is_null (bool, optional):  Whether to treat a zero length array as a null. Defaults to True.
    """

    zero_length_expr = ""
    if zero_length_is_null:
        zero_length_expr = (
            f"when size({col_name}_l) = 0 or size({col_name}_r) = 0 then -1"
        )

    c = f"""case
    when {col_name}_l is null or {col_name}_r is null then -1
    {zero_length_expr}
    when size(array_intersect({col_name}_l, {col_name}_r)) > 1 then 2
    when size(array_intersect({col_name}_l, {col_name}_r)) = 1 then 1
    else 0
    end
    """

    return _add_as_gamma_to_case_statement(c, gamma_col_name)


def _daexplode(col_name):
    return f"DualArrayExplode({col_name}_l, {col_name}_r)"


def _score_all_pairwise_combinations_distance(col_name, fn_name):

    return f"""
        transform({_daexplode(col_name)},
            x -> {fn_name}(x['_1'], x['_2'] )
            )
    """


def _compare_pairwise_transformed_combinations(col_name, fn_name):

    return f"""
        transform(
            {_daexplode(col_name)},
            x -> {fn_name}(x['_1']) = {fn_name}(x['_2'])
            )
    """


def _compare_pairwise_combinations_leven_prop(col_name):
    return f"""
        transform(
            {_daexplode(col_name)},
            x -> levenshtein(x['_1'], x['_2'])/((length(x['_1']) + length(x['_2']))/2)
            )
    """


def _jaro_winkler_array(col_name):
    return _score_all_pairwise_combinations_distance(col_name, "jaro_winkler_sim")


def _leven_array(col_name):
    return _score_all_pairwise_combinations_distance(col_name, "levenshtein")


def _dmeta_array(col_name):
    # Is at least one true
    return f"""exists(
    {_compare_pairwise_transformed_combinations(col_name, "Dmetaphone")},
    x -> x
    )
    """


def _size_intersect(col_name):
    return f"size(array_intersect({col_name}_l, {col_name}_r))"


def sql_gen_case_stmt_array_combinations_leven_abs_3(
    col_name: str,
    threshold1: int = 0,
    threshold2: int = 2,
    gamma_col_name=None,
    zero_length_is_null=True,
):
    """Compare all combinations of values in input arrays.  Gamma level 2 if minimum levenshtein score is <=
    threshold1.  Gamma level 1 if min score is  <= threshold2.  Otherwise level 0

    Args:
        col_name (str): The name of the column we want to generate a custom case expression for e.g. surname
        threshold1 (int, optional):  Defaults to 1.
        threshold2 (int, optional):  Defaults to 2.
        gamma_col_name (str, optional): . The name of the column, for the alias e.g. surname
        zero_length_is_null (bool, optional):  Whether to treat a zero length array as a null. Defaults to True.
    """

    zero_length_expr = ""
    if zero_length_is_null:
        zero_length_expr = (
            f"when size({col_name}_l) = 0 or size({col_name}_r) = 0 then -1"
        )

    c = f"""case
    when {col_name}_l is null or {col_name}_r is null then -1
    {zero_length_expr}
    when {_size_intersect(col_name)} >= 1 then 2
    when array_min({_leven_array(col_name)}) <= {threshold1} then 2
    when array_min({_leven_array(col_name)}) <= {threshold2} then 1
    else 0
    end
    """

    return _add_as_gamma_to_case_statement(c, gamma_col_name)


def sql_gen_case_stmt_array_combinations_leven_rel_3(
    col_name: str,
    threshold1: float = 0.0,
    threshold2: float = 0.1,
    gamma_col_name=None,
    zero_length_is_null=True,
):
    """Compare all combinations of values in input arrays.  Gamma level 2 if levenshtein score as
    percentage of average length is <=threshold1.  Gamma level 1 if min score is  <= threshold2.
    Otherwise level 0

    Args:
        col_name (str): The name of the column we want to generate a custom case expression for e.g. surname
        threshold1 (int, optional):  Defaults to 0.05 (i.e. 5%)
        threshold2 (int, optional):  Defaults to 0.1 (i.e. 10%)
        gamma_col_name (str, optional): . The name of the column, for the alias e.g. surname
        zero_length_is_null (bool, optional):  Whether to treat a zero length array as a null. Defaults to True.
    """

    zero_length_expr = ""
    if zero_length_is_null:
        zero_length_expr = (
            f"when size({col_name}_l) = 0 or size({col_name}_r) = 0 then -1"
        )

    c = f"""case
    when {col_name}_l is null or {col_name}_r is null then -1
    {zero_length_expr}
    when {_size_intersect(col_name)} >= 1 then 2
    when array_min({_compare_pairwise_combinations_leven_prop(col_name)}) <= {threshold1} then 2
    when array_min({_compare_pairwise_combinations_leven_prop(col_name)}) <= {threshold2} then 1
    else 0
    end
    """

    return _add_as_gamma_to_case_statement(c, gamma_col_name)


def sql_gen_case_stmt_array_combinations_jaro_3(
    col_name: str,
    threshold1=1.0,
    threshold2=0.88,
    gamma_col_name=None,
    zero_length_is_null=True,
):
    """Compare all combinations of values in input arrays.  Gamma level 2 if max jaro_winkler score is >=
    threshold1.  Gamma level 1 if max score is  >= threshold2.  Otherwise level 0

    Args:
        col_name (str): The name of the column we want to generate a custom case expression for e.g. surname
        threshold1 (int, optional):  Defaults to 1.
        threshold2 (int, optional):  Defaults to 0.88.
        gamma_col_name (str, optional): . The name of the column, for the alias e.g. surname
        zero_length_is_null (bool, optional):  Whether to treat a zero length array as a null. Defaults to True.
    """

    zero_length_expr = ""
    if zero_length_is_null:
        zero_length_expr = (
            f"when size({col_name}_l) = 0 or size({col_name}_r) = 0 then -1"
        )

    c = f"""case
    when {col_name}_l is null or {col_name}_r is null then -1
    {zero_length_expr}
    when {_size_intersect(col_name)} >= 1 then 2
    when array_max({_jaro_winkler_array(col_name)}) >= {threshold1} then 2
    when array_max({_jaro_winkler_array(col_name)}) >= {threshold2} then 1
    else 0
    end

    """

    return _add_as_gamma_to_case_statement(c, gamma_col_name)


def sql_gen_case_stmt_array_combinations_jaro_dmeta_4(
    col_name: str,
    threshold1=1.0,
    threshold2=0.88,
    gamma_col_name=None,
    zero_length_is_null=True,
):
    """Compare all combinations of values in input arrays.
    Gamma level 3 if max jaro_winkler score is >= threshold1
    Gamma level 2 if there's at least one match on dmetaphone
    Gamma level 1 if max jaro_winkler score is >= threshold2
    else Gamma level 0

    Args:
        col_name (str): The name of the column we want to generate a custom case expression for e.g. surname
        threshold1 (int, optional):  Defaults to 1.0.
        threshold2 (int, optional):  Defaults to 0.88.
        gamma_col_name (str, optional): . The name of the column, for the alias e.g. surname
        zero_length_is_null (bool, optional):  Whether to treat a zero length array as a null. Defaults to True.
    """

    zero_length_expr = ""
    if zero_length_is_null:
        zero_length_expr = (
            f"when size({col_name}_l) = 0 or size({col_name}_r) = 0 then -1"
        )

    c = f"""case
    when {col_name}_l is null or {col_name}_r is null then -1
    {zero_length_expr}
    when {_size_intersect(col_name)} >= 1 then 3
    when array_max({_jaro_winkler_array(col_name)}) >= {threshold1} then 3
    when {_dmeta_array(col_name)} then 2
    when array_max({_jaro_winkler_array(col_name)}) >= {threshold2} then 1
    else 0
    end

    """

    return _add_as_gamma_to_case_statement(c, gamma_col_name)
