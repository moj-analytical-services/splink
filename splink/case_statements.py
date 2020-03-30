import re
import warnings

def _check_jaro_registered(spark):

    if spark is None:
        return False

    if spark == 'supress_warnings':  # Allows us to surpress the warning in the test suite
        return False

    for fn in spark.catalog.listFunctions():
        if fn.name == 'jaro_winkler_sim':
            return True


    warnings.warn(f"The jaro_winkler_sim user definined function is not available in Spark "
                        "Or you did not pass 'spark' (the SparkSession) into 'Params' "
                        "Falling back to using levenshtein in the default string comparison functions "
                        "You can import these functions using the scala-udf-similarity-0.0.6.jar provided with Splink")
    return False


def _add_as_gamma_to_case_statement(case_statement: str, gamma_col_name):
    """As the correct column alias to the case statement if it does not exist

    Args:
        case_statement (str): Original case statement

    Returns:
        str: case_statement with correct alias
    """

    sl = case_statement.lower()
    # What we're trying to do is distinguish between 'case when end as gamma_blah' from case when end
    sl = sl.replace('\n', ' ').replace('\r', '')
    sl = sl.strip()
    # Only need to do anything if the last non-blank string is not ' end'
    if sl[-4:] != ' end':
        # The case statement has a 'end as gamma blah'
        last_end_index = sl.rfind(" end ")
        sl = sl[:last_end_index + 5]
    return f"{sl} as gamma_{gamma_col_name}"

def _check_no_obvious_problem_with_case_statement(case_statement):
    seems_valid = True
    cs_l = case_statement.lower()
    if 'case' not in cs_l:
        seems_valid = False
    if 'end' not in cs_l:
        seems_valid = False
    if 'when' not in cs_l:
        seems_valid = False
    if 'then' not in cs_l:
        seems_valid = False

    if not seems_valid:
        s1 = "The case expression you provided does not seem to be valid SQL."
        s2 = f"Expression provided is: '{case_statement}'"
        raise ValueError(f"{s1} {s2}")

def sql_gen_case_smnt_strict_equality_2(col_name, gamma_col_name=None):
    c = f"""case
    when {col_name}_l is null or {col_name}_r is null then -1
    when {col_name}_l = {col_name}_r then 1
    else 0 end as gamma_{gamma_col_name}"""

    if gamma_col_name is not None:
        return _add_as_gamma_to_case_statement(c, gamma_col_name)
    else:
        return c



# Sting comparison select statements

# jaro winkler statements, using param values suggested on page 355 of  https://imai.fas.harvard.edu/research/files/linkage.pdf
# American Political Science Review (2019) 113, 2, 353–371, Using a Probabilistic Model to Assist Merging of Large-Scale
# Administrative Records

def sql_gen_gammas_case_stmt_jaro_2(col_name, gamma_col_name=None, threshold=0.94):
    c = f"""case
    when {col_name}_l is null or {col_name}_r is null then -1
    when jaro_winkler_sim({col_name}_l, {col_name}_r) > {threshold} then 1
    else 0 end"""
    if gamma_col_name is not None:
        return _add_as_gamma_to_case_statement(c, gamma_col_name)
    else:
        return c


def sql_gen_gammas_case_stmt_jaro_3(col_name, gamma_col_name=None, threshold1=0.94, threshold2=0.88):
    c = f"""case
    when {col_name}_l is null or {col_name}_r is null then -1
    when jaro_winkler_sim({col_name}_l, {col_name}_r) > {threshold1} then 2
    when jaro_winkler_sim({col_name}_l, {col_name}_r) > {threshold2} then 1
    else 0 end"""
    if gamma_col_name is not None:
        return _add_as_gamma_to_case_statement(c, gamma_col_name)
    else:
        return c

def sql_gen_gammas_case_stmt_jaro_4(col_name, gamma_col_name=None, threshold1=0.94, threshold2=0.88, threshold3=0.7):
    c = f"""case
    when {col_name}_l is null or {col_name}_r is null then -1
    when jaro_winkler_sim({col_name}_l, {col_name}_r) > {threshold1} then 3
    when jaro_winkler_sim({col_name}_l, {col_name}_r) > {threshold2} then 2
    when jaro_winkler_sim({col_name}_l, {col_name}_r) > {threshold3} then 1
    else 0 end"""
    if gamma_col_name is not None:
        return _add_as_gamma_to_case_statement(c, gamma_col_name)
    else:
        return c


# levenshtein fallbacks for if string similarity jar isn't available
def sql_gen_case_stmt_levenshtein_3(col_name, gamma_col_name=None, threshold=0.3):
    c = f"""case
    when {col_name}_l is null or {col_name}_r is null then -1
    when {col_name}_l = {col_name}_r then 2
    when levenshtein({col_name}_l, {col_name}_r)/((length({col_name}_l) + length({col_name}_r))/2) <= {threshold}
    then 1
    else 0 end"""
    if gamma_col_name is not None:
        return _add_as_gamma_to_case_statement(c, gamma_col_name)
    else:
        return c

def sql_gen_case_stmt_levenshtein_4(col_name, gamma_col_name=None, threshold1=0.2, threshold2=0.4):
    c = f"""case
    when {col_name}_l is null or {col_name}_r is null then -1
    when {col_name}_l = {col_name}_r then 3
    when levenshtein({col_name}_l, {col_name}_r)/((length({col_name}_l) + length({col_name}_r))/2) <= {threshold1}
    then 2
    when levenshtein({col_name}_l, {col_name}_r)/((length({col_name}_l) + length({col_name}_r))/2) <= {threshold2}
    then 1
    else 0 end"""
    if gamma_col_name is not None:
        return _add_as_gamma_to_case_statement(c, gamma_col_name)
    else:
        return c



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

def sql_gen_case_stmt_numeric_2(col_name, gamma_col_name=None):

    col1 =  f"{col_name}_l"
    col2 =  f"{col_name}_r"

    abs_difference = _sql_gen_abs_diff(col1, col2)

    c = f"""case
    when {col_name}_l is null or {col_name}_r is null then -1
    when {abs_difference} < 0.00001 then 1
    else 0 end"""
    if gamma_col_name is not None:
        return _add_as_gamma_to_case_statement(c, gamma_col_name)
    else:
        return c


def sql_gen_case_stmt_numeric_abs_3(col_name, gamma_col_name=None, abs_amount=1, equality_threshold=0.0001):

    col1 =  f"{col_name}_l"
    col2 =  f"{col_name}_r"

    abs_difference = _sql_gen_abs_diff(col1, col2)

    c = f"""case
    when {col_name}_l is null or {col_name}_r is null then -1
    when {abs_difference} < {equality_threshold} THEN 2
    when {abs_difference} < {abs_amount} THEN 1
    else 0 end"""
    if gamma_col_name is not None:
        return _add_as_gamma_to_case_statement(c, gamma_col_name)
    else:
        return c

def sql_gen_case_stmt_numeric_abs_4(col_name, gamma_col_name=None, abs_amount_low = 1, abs_amount_high = 10, equality_threshold=0.0001):

    col1 =  f"{col_name}_l"
    col2 =  f"{col_name}_r"

    abs_difference = _sql_gen_abs_diff(col1, col2)

    c = f"""case
    when {col_name}_l is null or {col_name}_r is null then -1
    when {abs_difference} < {equality_threshold} THEN 3
    when {abs_difference} < {abs_amount_low} THEN 2
    when {abs_difference} < {abs_amount_high} THEN 1
    else 0 end"""
    if gamma_col_name is not None:
        return _add_as_gamma_to_case_statement(c, gamma_col_name)
    else:
        return c


def sql_gen_case_stmt_numeric_perc_3(col_name, gamma_col_name=None, per_diff=0.05, equality_threshold=0.0001):

    col1 =  f"{col_name}_l"
    col2 =  f"{col_name}_r"

    max_of_cols = _sql_gen_max_of_two_cols(col1, col2)
    abs_difference = _sql_gen_abs_diff(col1, col2)

    c = f"""case
    when {col_name}_l is null or {col_name}_r is null then -1
    when {abs_difference}/abs({max_of_cols}) < {equality_threshold} then 2
    when {abs_difference}/abs({max_of_cols}) < {per_diff} then 1
    else 0 end"""
    if gamma_col_name is not None:
        return _add_as_gamma_to_case_statement(c, gamma_col_name)
    else:
        return c

def sql_gen_case_stmt_numeric_perc_4(col_name, gamma_col_name=None, per_diff_low = 0.05, per_diff_high=0.10, equality_threshold=0.0001):

    col1 =  f"{col_name}_l"
    col2 =  f"{col_name}_r"

    max_of_cols = _sql_gen_max_of_two_cols(col1, col2)
    abs_difference = _sql_gen_abs_diff(col1, col2)

    c =  f"""case
    when {col_name}_l is null or {col_name}_r is null then -1
    when {abs_difference}/abs({max_of_cols}) < {equality_threshold} then 3
    when {abs_difference}/abs({max_of_cols}) < {per_diff_low} then 2
    when {abs_difference}/abs({max_of_cols}) < {per_diff_high} then 1
    else 0 end"""
    if gamma_col_name is not None:
        return _add_as_gamma_to_case_statement(c, gamma_col_name)
    else:
        return c

def _sql_gen_get_or_list(col_name, other_name_cols, threshold=0.94):
    # Note the ifnull 1234 just ensures that if one of the other columns is null, the jaro score is lower than the threshold
    ors = [f"jaro_winkler_sim({col_name}_l, ifnull({n}_r, '1234')) > {threshold}" for n in other_name_cols]
    ors_string = " OR ".join(ors)
    return f"({ors_string})"

def sql_gen_gammas_name_inversion_4(col_name:str, other_name_cols:list, gamma_col_name=None, threshold1=0.94, threshold2=0.88):
    """Generate a case expression which can handle name inversions where e.g. surname and forename are inverted
    
    Args:
        col_name (str): The name of the column we want to generate a custom case expression for e.g. surname
        other_name_cols (list): The name of the other columns that contain names e.g. forename1, forename2
        gamma_col_name (str, optional): . The name of the column, for the alias e.g. surname
        threshold1 (float, optional): Jaro threshold for almost exact match. Defaults to 0.94.
        threshold2 (float, optional): Jaro threshold for close match Defaults to 0.88.
    
    Returns:
        str: A sql string
    """    
       
    c = f"""case
    when {col_name}_l is null or {col_name}_r is null then -1
    when jaro_winkler_sim({col_name}_l, {col_name}_r) > {threshold1} then 3
    when {_sql_gen_get_or_list(col_name, other_name_cols, threshold1)} then 2
    when jaro_winkler_sim({col_name}_l, {col_name}_r) > {threshold2} then 1
    else 0 end"""
    if gamma_col_name is not None:
        return _add_as_gamma_to_case_statement(c, gamma_col_name)
    else:
        return c