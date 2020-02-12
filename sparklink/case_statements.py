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
                        "You can import these functions using the scala-udf-similarity-0.0.6.jar provided with Sparklink")
    return False


def _add_null_treatment_to_case_statement(case_statement: str):
    """Add null treatment to user provided case statement if not already exists

    Args:
        case_statement (str): The select case statement we want to add null treatment to

    Returns:
        str: case statement with null treatment added
    """

    sl = case_statement.lower()

    if "then -1" not in sl:
        try:
            variable_name = re.search(r"when ([\w_]{1,100})_l", case_statement)[1]
        except:
            raise ValueError(("Your case statement needs to reference a variable on the left hand "
                              "side of the comparison i.e. a variable ending with _l "
                             f"current case statement: \n{case_statement}"))
        find = r"(case)(\s+)(when)"
        replace = r"\1 \nwhen {col_name}_l is null or {col_name}_r is null then -1\n\3"
        new_case_statement = re.sub(find, replace, case_statement)
        new_case_statement = new_case_statement.format(col_name=variable_name)

        return new_case_statement
    else:
        return case_statement


def _add_as_gamma_to_case_statement(case_statement: str, gamma_index):
    """As the correct column alias to the case statement if it does not exist

    Args:
        case_statement (str): Original case statement

    Returns:
        str: case_statement with correct alias
    """

    sl = case_statement.lower()
    last_end_index = sl.rfind("end")
    sl = sl[:last_end_index + 3]
    return f"{sl} as gamma_{gamma_index}"

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

def sql_gen_case_smnt_strict_equality_2(col_name, gamma_index=None):
    c = f"""case
    when {col_name}_l is null or {col_name}_r is null then -1
    when {col_name}_l = {col_name}_r then 1
    else 0 end as gamma_{gamma_index}"""

    if gamma_index is not None:
        return _add_as_gamma_to_case_statement(c, gamma_index)
    else:
        return c



# Sting comparison select statements

# jaro winkler statements, using param values suggested on page 355 of  https://imai.fas.harvard.edu/research/files/linkage.pdf
# American Political Science Review (2019) 113, 2, 353–371, Using a Probabilistic Model to Assist Merging of Large-Scale
# Administrative Records

def sql_gen_gammas_case_stmt_jaro_2(col_name, gamma_index=None, threshold=0.94):
    c = f"""case
    when {col_name}_l is null or {col_name}_r is null then -1
    when jaro_winkler_sim({col_name}_l, {col_name}_r) > {threshold} then 1
    else 0 end"""
    if gamma_index is not None:
        return _add_as_gamma_to_case_statement(c, gamma_index)
    else:
        return c


def sql_gen_gammas_case_stmt_jaro_3(col_name, gamma_index=None, threshold1=0.94, threshold2=0.88):
    c = f"""case
    when {col_name}_l is null or {col_name}_r is null then -1
    when jaro_winkler_sim({col_name}_l, {col_name}_r) > {threshold1} then 2
    when jaro_winkler_sim({col_name}_l, {col_name}_r) > {threshold2} then 1
    else 0 end"""
    if gamma_index is not None:
        return _add_as_gamma_to_case_statement(c, gamma_index)
    else:
        return c

def sql_gen_gammas_case_stmt_jaro_4(col_name, gamma_index=None, threshold1=0.94, threshold2=0.88, threshold3=0.7):
    c = f"""case
    when {col_name}_l is null or {col_name}_r is null then -1
    when jaro_winkler_sim({col_name}_l, {col_name}_r) > {threshold1} then 3
    when jaro_winkler_sim({col_name}_l, {col_name}_r) > {threshold2} then 2
    when jaro_winkler_sim({col_name}_l, {col_name}_r) > {threshold3} then 1
    else 0 end"""
    if gamma_index is not None:
        return _add_as_gamma_to_case_statement(c, gamma_index)
    else:
        return c


# levenshtein fallbacks for if string similarity jar isn't available
def sql_gen_case_stmt_levenshtein_3(col_name, gamma_index=None, threshold=0.3):
    c = f"""case
    when {col_name}_l is null or {col_name}_r is null then -1
    when {col_name}_l = {col_name}_r then 2
    when levenshtein({col_name}_l, {col_name}_r)/((length({col_name}_l) + length({col_name}_r))/2) <= {threshold}
    then 1
    else 0 end"""
    if gamma_index is not None:
        return _add_as_gamma_to_case_statement(c, gamma_index)
    else:
        return c

def sql_gen_case_stmt_levenshtein_4(col_name, gamma_index=None, threshold1=0.2, threshold2=0.4):
    c = f"""case
    when {col_name}_l is null or {col_name}_r is null then -1
    when {col_name}_l = {col_name}_r then 3
    when levenshtein({col_name}_l, {col_name}_r)/((length({col_name}_l) + length({col_name}_r))/2) <= {threshold1}
    then 2
    when levenshtein({col_name}_l, {col_name}_r)/((length({col_name}_l) + length({col_name}_r))/2) <= {threshold2}
    then 1
    else 0 end"""
    if gamma_index is not None:
        return _add_as_gamma_to_case_statement(c, gamma_index)
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

def sql_gen_case_stmt_numeric_2(col_name, gamma_index=None):

    col1 =  f"{col_name}_l"
    col2 =  f"{col_name}_r"

    abs_difference = _sql_gen_abs_diff(col1, col2)

    c = f"""case
    when {col_name}_l is null or {col_name}_r is null then -1
    when {abs_difference} < 0.00001 then 1
    else 0 end"""
    if gamma_index is not None:
        return _add_as_gamma_to_case_statement(c, gamma_index)
    else:
        return c


def sql_gen_case_stmt_numeric_abs_3(col_name, gamma_index=None, abs_amount=1, equality_threshold=0.0001):

    col1 =  f"{col_name}_l"
    col2 =  f"{col_name}_r"

    abs_difference = _sql_gen_abs_diff(col1, col2)

    c = f"""case
    when {col_name}_l is null or {col_name}_r is null then -1
    when {abs_difference} < {equality_threshold} THEN 2
    when {abs_difference} < {abs_amount} THEN 1
    else 0 end"""
    if gamma_index is not None:
        return _add_as_gamma_to_case_statement(c, gamma_index)
    else:
        return c

def sql_gen_case_stmt_numeric_abs_4(col_name, gamma_index=None, abs_amount_low = 1, abs_amount_high = 10, equality_threshold=0.0001):

    col1 =  f"{col_name}_l"
    col2 =  f"{col_name}_r"

    abs_difference = _sql_gen_abs_diff(col1, col2)

    c = f"""case
    when {col_name}_l is null or {col_name}_r is null then -1
    when {abs_difference} < {equality_threshold} THEN 3
    when {abs_difference} < {abs_amount_low} THEN 2
    when {abs_difference} < {abs_amount_high} THEN 1
    else 0 end"""
    if gamma_index is not None:
        return _add_as_gamma_to_case_statement(c, gamma_index)
    else:
        return c


def sql_gen_case_stmt_numeric_perc_3(col_name, gamma_index=None, per_diff=0.05, equality_threshold=0.0001):

    col1 =  f"{col_name}_l"
    col2 =  f"{col_name}_r"

    max_of_cols = _sql_gen_max_of_two_cols(col1, col2)
    abs_difference = _sql_gen_abs_diff(col1, col2)

    c = f"""case
    when {col_name}_l is null or {col_name}_r is null then -1
    when {abs_difference}/abs({max_of_cols}) < {equality_threshold} then 2
    when {abs_difference}/abs({max_of_cols}) < {per_diff} then 1
    else 0 end"""
    if gamma_index is not None:
        return _add_as_gamma_to_case_statement(c, gamma_index)
    else:
        return c

def sql_gen_case_stmt_numeric_perc_4(col_name, gamma_index=None, per_diff_low = 0.05, per_diff_high=0.10, equality_threshold=0.0001):

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
    if gamma_index is not None:
        return _add_as_gamma_to_case_statement(c, gamma_index)
    else:
        return c

