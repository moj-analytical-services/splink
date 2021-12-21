import re
from splink.blocking import _sql_gen_where_condition, block_using_rules

from copy import deepcopy

from pyspark.sql import DataFrame

def get_queryplan_text(df, blocking_rule, splink_settings):
    spark = df.sql_ctx.sparkSession

    # Temporarily override broadcast join threshold to ensure
    # that the query plan is a SortMergeJoin
    current_setting = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

    link_type = splink_settings["link_type"]
    if link_type == "dedupe_only":
        source_dataset_col = None
    else:
        source_dataset_col = splink_settings["source_dataset_column_name"]

    unique_id_col = splink_settings["unique_id_col"]

    join_filter = _sql_gen_where_condition(link_type, source_dataset_col, unique_id_col)


    sql = f"""
    select * from
    df as l
    inner join df as r
    on {blocking_rule}  {join_filter}
    """

    df = spark.sql(sql)
    qe = df._jdf.queryExecution()
    sp = qe.sparkPlan()
    treestring= sp.treeString()
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", current_setting)
    return treestring



def get_join_line(queryplan_text):
    lines = queryplan_text.splitlines()
    return lines[0]

def parse_join_line_sortmergejoin(join_line):
    parts = split_by_commas_ignoring_within_brackets(join_line)
    hash_columns = get_hash_columns(join_line)

    if len(parts) == 4:
        post_join_filters = remove_col_ids(parts[3])
    else:
        post_join_filters = None


    return {
        "join_strategy": "SortMergeJoin",
        "join_type": "Inner",
        "join_hashpartition_columns_left": hash_columns["left"],
        "join_hashpartition_columns_right": hash_columns["right"],
        "post_join_filters": post_join_filters,
    }

def parse_join_line_cartesian(join_line):
    fil  = extract_text_from_within_brackets_balanced(join_line, ["(", ")"])
    fil = remove_col_ids(fil)
    return {
        "join_strategy": "Cartesian",
        "join_type": "Cartesian",
        "join_hashpartition_columns_left": [],
        "join_hashpartition_columns_right": [],
        "post_join_filters": fil,
    }

def parse_join_line(join_line):
    if "SortMergeJoin" in join_line:
        return parse_join_line_sortmergejoin(join_line)
    if "Cartesian" in join_line:
        return parse_join_line_cartesian(join_line)



def split_by_commas_ignoring_within_brackets(input_str):
    counter = 0
    captured_strings = []
    captured_string = ""
    for i in input_str:
        captured_string += i

        if i in ("(", "[", "{"):
            counter += 1

        if i in (")", "]", "}"):
            counter -= 1
        if counter == 0 and i == ",":
            captured_string = captured_string[:-1]
            captured_strings.append(captured_string)
            captured_string = ""
    captured_strings.append(captured_string)
    captured_strings = [s.strip() for s in captured_strings]
    return captured_strings


def extract_text_from_within_brackets_balanced(input_str, bracket_type=["[", "]"]):
    bracket_counter = 0

    start_bracket = bracket_type[0]
    end_bracket = bracket_type[1]

    captured_string = ""

    if start_bracket not in input_str:
        return None

    for i in input_str:

        if i == start_bracket:
            bracket_counter += 1

        if i == end_bracket:
            bracket_counter -= 1
            if bracket_counter == 0:
                break
        if bracket_counter > 0:
            captured_string += i

    return captured_string[1:]


def remove_col_ids(input_str):
    return re.sub(r"#\d{1,6}[L]?", "", input_str)


def _sorted_array(df: DataFrame, field_list: list):
    """Create a new field called _sorted_array
    containing a sorted array populated with the values from field_list
    Args:
        df (DataFrame): Input dataframe
        field_list (list): List of fields e.g. ["first_name", "surname"]
    Returns:
        df, with a new field called _sorted_array
    """

    df = df.withColumn("_sorted_array", F.array(*field_list))
    df = df.withColumn("_sorted_array", F.sort_array("_sorted_array"))
    return df


def _sort_fields(df: DataFrame, field_list_to_sort: list):
    """
    Take the fields in field_list and derive new fields
    with the same values but sorted alphabetically.
    The derieved fields are named __sorted_{field}
    Args:
        df (DataFrame): Input dataframe
        field_list_to_sort (list): list of fields e.g. ["first_name", "surname"]
    Returns:
        DataFrame: dataframe with new fields __sorted_{field}
    """

    df = _sorted_array(df, field_list_to_sort)

    for i, field in enumerate(field_list_to_sort):
        df = df.withColumn(f"__sorted_{field}", F.col("_sorted_array")[i])
    df = df.drop((f"_sorted_array"))
    return df


def get_total_comparisons_from_join_columns_that_will_be_hash_partitioned(df, join_cols: list):
    """Compute the total number of records that will be generated
    by an inner joi on the join_cols
    For instance, if join_cols = ["first_name", "dmetaphone(surname)"]
    will compute the total number of comparisons generated by the blocking rule:
    ["l.first_name = r.first_name and dmetaphone(l.surname) = dmetaphone(r.surname)"]
    Args:
        df (DataFrame): Input dataframe
        join_cols (list): List of blocking columns e.g. ["first_name", "dmetaphone(surname)"]
    Returns:
        integer: Number of comparisons generated by the blocking rule
    """

    sel_expr = ", ".join(join_cols)
    concat_expr = f"concat({sel_expr})"
    spark = df.sql_ctx.sparkSession

    df.createOrReplaceTempView("df")
    sql = f"""
    with
    block_groups as (
        SELECT {concat_expr}, {sel_expr},
        count(*) * count(*) as num_comparisons
    FROM df
    where {concat_expr} is not null
    GROUP BY {concat_expr}, {sel_expr}
    )
    select sum(num_comparisons) as total_comparisons
    from block_groups
    """

    return spark.sql(sql).collect()[0]["total_comparisons"]

def generate_and_count_num_comparisons_from_blocking_rule(df, blocking_rule, splink_settings):
    spark = df.sql_ctx.sparkSession
    splink_settings = deepcopy(splink_settings)
    splink_settings["blocking_rules"] = [blocking_rule]

    df = block_using_rules(splink_settings, df, spark)

    return df.count()


def get_hash_columns(smj_line):
    comma_split = split_by_commas_ignoring_within_brackets(smj_line)
    join_left = extract_text_from_within_brackets_balanced(comma_split[0])
    join_left = remove_col_ids(join_left)
    join_left = split_by_commas_ignoring_within_brackets(join_left)

    join_right = extract_text_from_within_brackets_balanced(comma_split[1] )
    join_right = remove_col_ids(join_right)
    join_right = split_by_commas_ignoring_within_brackets(join_right)
    return {"left": join_left, "right": join_right}



def get_num_comparisons_from_blocking_rule(df, blocking_rule, splink_settings):
    # Use n strategy if the blocking rule is symmetric
    # Use n^2 strategy when inversions are present
    # Reults as dict with num comparisons, hash columns, filter columns, join strategy reported

    smj_line = get_sortmergejoin_query_plan_text(df, blocking_rule)
    hc = get_hash_columns(smj_line)
    left_hash_cols = hc["left"]
    right_hash_cols = hc["right"]

    if left_hash_cols == right_hash_cols:
        return get_total_comparisons_from_join_columns_that_will_be_hashed(df, left_hash_cols)
    else:
        return generate_and_count_num_comparisons_from_blocking_rule(df, blocking_rule, splink_settings)






def analyse_blocking_rule(df, blocking_rule, splink_settings, compute_exact_comparisons=False, compute_exact_limit=1e9, compute_largest_block=True):

    queryplan_text = get_queryplan_text(df, blocking_rule, splink_settings)
    parsed = parse_join_line(get_join_line(queryplan_text))

    if parse_join_line["join_strategy"] == 'SortMergeJoin':

        jcl = parsed["join_hashpartition_columns_left"]
        jcr = parsed["join_hashpartition_columns_right"]
        balanced_join = (jcl == jcr)
        if balanced_join:
            total_comparisons_generated = get_total_comparisons_from_join_columns_that_will_be_hash_partitioned(df, hp_cols)
            parsed["comparisons_generated_before_filter_applied"] = total_comparisons_generated
        else:
            parsed["comparisons_generated_before_filter_applied"] = "Join columns include invesions, so cannot be computed"


    if parse_join_line["join_strategy"] == 'Cartesian':
        raw_count = df.count()
        parsed["comparisons_generated_before_filter_applied"] = raw_count * raw_count

    if compute_exact_comparisons and total_comparisons_generated<compute_exact_limit:
        blocked = block_using_rules(splink_settings, df, spark)
        total_with_filters = blocked.count()
        parsed["total_comparisons_after_filters_applied"] = total_with_filters
    else:
        parsed["total_comparisons_after_filters_applied"] = "Not computed, set compute_exact_comparisons=True to compute."

    return parsed


