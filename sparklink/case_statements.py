import re

def _check_jaro_registered(spark):
    for fn in spark.catalog.listFunctions():
        if fn.name == 'jaro_winkler_sim':
            return True
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
        variable_name = re.search(r"when ([\w_]{1,100})_l", case_statement)[1]
        find = r"(case)(\s+)(when)"
        replace = r"\1 \nwhen {col_name}_l is null or {col_name}_r is null then -1\n\3"
        new_case_statement = re.sub(find, replace, case_statement)
        new_case_statement = new_case_statement.format(col_name=variable_name)

        return new_case_statement
    else:
        return case_statement


def sql_gen_case_smnt_strict_equality_2(col_name, i):
    return f"""case
    when {col_name}_l is null or {col_name}_r is null then -1
    when {col_name}_l = {col_name}_r then 1
    else 0 end as gamma_{i}"""

# Sting comparison select statements

# jaro winkler statements, using param values suggested on page 355 of  https://imai.fas.harvard.edu/research/files/linkage.pdf
# American Political Science Review (2019) 113, 2, 353–371, Using a Probabilistic Model to Assist Merging of Large-Scale
# Administrative Records

def sql_gen_gammas_case_stmt_jaro_2(col_name, i, threshold=0.94):
    return f"""case
    when {col_name}_l is null or {col_name}_r is null then -1
    when jaro_winkler_sim({col_name}_l, {col_name}_r) > {threshold} then 1
    else 0 end as gamma_{i}"""


def sql_gen_gammas_case_stmt_jaro_3(col_name, i, threshold1=0.94, threshold2=0.88):
    return f"""case
    when {col_name}_l is null or {col_name}_r is null then -1
    when jaro_winkler_sim({col_name}_l, {col_name}_r) > {threshold1} then 2
    when jaro_winkler_sim({col_name}_l, {col_name}_r) > {threshold2} then 1
    else 0 end as gamma_{i}"""

def sql_gen_gammas_case_stmt_jaro_4(col_name, i, threshold1=0.94, threshold2=0.88, threshold3=0.7):
    return f"""case
    when {col_name}_l is null or {col_name}_r is null then -1
    when jaro_winkler_sim({col_name}_l, {col_name}_r) > {threshold1} then 3
    when jaro_winkler_sim({col_name}_l, {col_name}_r) > {threshold2} then 2
    when jaro_winkler_sim({col_name}_l, {col_name}_r) > {threshold3} then 1
    else 0 end as gamma_{i}"""


# levenshtein fallbacks for if string similarity jar isn't available
def sql_gen_case_stmt_levenshtein_3(col_name, i, threshold=0.3):
    return f"""case
    when {col_name}_l is null or {col_name}_r is null then -1
    when {col_name}_l = {col_name}_r then 2
    when levenshtein({col_name}_l, {col_name}_r)/((length({col_name}_l) + length({col_name}_r))/2) <= {threshold}
    then 1
    else 0 end as gamma_{i}"""

def sql_gen_case_stmt_levenshtein_4(col_name, i, threshold1=0.2, threshold2=0.4):
    return f"""case
    when {col_name}_l is null or {col_name}_r is null then -1
    when {col_name}_l = {col_name}_r then 3
    when levenshtein({col_name}_l, {col_name}_r)/((length({col_name}_l) + length({col_name}_r))/2) <= {threshold1}
    then 2
    when levenshtein({col_name}_l, {col_name}_r)/((length({col_name}_l) + length({col_name}_r))/2) <= {threshold2}
    then 1
    else 0 end as gamma_{i}"""



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



def sql_gen_case_stmt_numeric_abs_3(col_name, abs_amount, i):

    col1 =  f"{col_name}_l"
    col2 =  f"{col_name}_r"

    abs_difference = _sql_gen_abs_diff(col1, col2)

    return f"""case
    when {col_name}_l is null or {col_name}_r is null then -1
    when {abs_difference} < 0.0001 THEN 2  /* 0.0001 to account for floating point errors */
    when {abs_difference} < {abs_amount} THEN 1
    else 0 end as gamma_{i}"""

def sql_gen_case_stmt_numeric_abs_4(col_name, abs_amount_low, abs_amount_high, i):

    col1 =  f"{col_name}_l"
    col2 =  f"{col_name}_r"

    abs_difference = _sql_gen_abs_diff(col1, col2)

    return f"""case
    when {col_name}_l is null or {col_name}_r is null then -1
    when {abs_difference} < 0.0001 THEN 3  /* 0.0001 to account for floating point errors */
    when {abs_difference} < {abs_amount_low} THEN 2
    when {abs_difference} < {abs_amount_high} THEN 1
    else 0 end as gamma_{i}"""


def sql_gen_case_stmt_numeric_perc_3(col_name, per_diff, i):

    col1 =  f"{col_name}_l"
    col2 =  f"{col_name}_r"

    max_of_cols = _sql_gen_max_of_two_cols(col1, col2)
    abs_difference = _sql_gen_abs_diff(col1, col2)

    return f"""case
    when {col_name}_l is null or {col_name}_r is null then -1
    when {abs_difference}/abs({max_of_cols}) < 0.00001 then 2
    when {abs_difference}/abs({max_of_cols}) < {per_diff} then 1
    else 0 end as gamma_{i}"""

def sql_gen_case_stmt_numeric_perc_4(col_name, per_diff_low, per_diff_high, i):

    col1 =  f"{col_name}_l"
    col2 =  f"{col_name}_r"

    max_of_cols = _sql_gen_max_of_two_cols(col1, col2)
    abs_difference = _sql_gen_abs_diff(col1, col2)

    return f"""case
    when {col_name}_l is null or {col_name}_r is null then -1
    when {abs_difference}/abs({max_of_cols}) < 0.00001 then 3
    when {abs_difference}/abs({max_of_cols}) < {per_diff_low} then 2
    when {abs_difference}/abs({max_of_cols}) < {per_diff_high} then 1
    else 0 end as gamma_{i}"""

