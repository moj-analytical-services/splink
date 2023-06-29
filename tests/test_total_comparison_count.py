import pandas as pd
import pytest

from splink.analyse_blocking import number_of_comparisons_generated_by_blocking_rule_sql
from splink.duckdb.linker import DuckDBLinker
from splink.misc import calculate_cartesian
from splink.vertically_concatenate import vertically_concatenate_sql


# convenience to get list into format as though it was result of a count query
def list_to_row_count(list_of_numbers):
    return [{"count": el} for el in list_of_numbers]


def test_calculate_cartesian_dedupe_only():
    # dedupe_only - have n(n-1)/2 comparisons, only a single frame
    assert calculate_cartesian(list_to_row_count([5]), "dedupe_only") == 10
    assert calculate_cartesian(list_to_row_count([8]), "dedupe_only") == 28
    assert calculate_cartesian(list_to_row_count([10]), "dedupe_only") == 45
    with pytest.raises(ValueError):
        calculate_cartesian(list_to_row_count([10, 20]), "dedupe_only")


def test_calculate_cartesian_link_only():
    # link_only - sum of all pairwise comparisons - i.e. sum of all pairwise products
    assert calculate_cartesian(list_to_row_count([2, 3]), "link_only") == 6
    assert calculate_cartesian(list_to_row_count([7, 11]), "link_only") == 77
    assert calculate_cartesian(list_to_row_count([2, 2, 2]), "link_only") == 12
    assert calculate_cartesian(list_to_row_count([2, 3, 5]), "link_only") == 31
    assert calculate_cartesian(list_to_row_count([1, 1, 1]), "link_only") == 3
    assert calculate_cartesian(list_to_row_count([2, 2, 2, 2, 2]), "link_only") == 40
    assert calculate_cartesian(list_to_row_count([5, 5, 5, 5]), "link_only") == 150
    with pytest.raises(ValueError):
        calculate_cartesian(list_to_row_count([12]), "link_only")


def test_calculate_cartesian_link_and_dedupe():
    # link_and_dedupe - much like dedupe,
    # N(N - 1)/2 comparisons with N = sum of rows of all frames
    # alternatively can think of this as
    # 'link only' links + dedupe links for each frame in list
    assert calculate_cartesian(list_to_row_count([8]), "link_and_dedupe") == 28
    assert calculate_cartesian(list_to_row_count([2, 3]), "link_and_dedupe") == 10
    assert (
        calculate_cartesian(list_to_row_count([7, 11]), "link_and_dedupe")
        == 77 + 21 + 55
    )
    assert calculate_cartesian(list_to_row_count([2, 2, 2]), "link_and_dedupe") == 15
    assert calculate_cartesian(list_to_row_count([1, 1, 1]), "link_and_dedupe") == 3
    assert (
        calculate_cartesian(list_to_row_count([2, 2, 2, 2, 2]), "link_and_dedupe") == 45
    )
    assert (
        calculate_cartesian(list_to_row_count([5, 5, 5, 5]), "link_and_dedupe") == 190
    )


@pytest.mark.parametrize(
    "link_type,frame_sizes,group_by",
    [
        ("dedupe_only", [97], ""),
        ("link_only", [209, 104, 97, 2], "group by source_dataset"),
        ("link_and_dedupe", [209, 104, 97, 2], "group by source_dataset"),
    ],
)
def test_calculate_cartesian_equals_total_number_of_links(
    link_type, frame_sizes, group_by
):
    # test that the count we get from calculate_cartesian
    # is the same as the actual number we get if we generate _all_ links
    # (i.e. using dummy blocking rule "1=1")

    def make_dummy_frame(row_count):
        # don't need meaningful differences as only interested in total count
        return pd.DataFrame(
            data={
                "unique_id": range(0, row_count),
                "forename": "Claire",
                "surname": "Brown",
            },
        )

    dfs = list(map(make_dummy_frame, frame_sizes))
    settings = {"link_type": link_type}

    linker = DuckDBLinker(dfs, settings)
    sql = vertically_concatenate_sql(linker)
    linker._enqueue_sql(sql, "__splink__df_concat")
    df_concat = linker._execute_sql_pipeline()

    # calculate full number of comparisons
    full_count_sql = number_of_comparisons_generated_by_blocking_rule_sql(linker, "1=1")
    linker._enqueue_sql(full_count_sql, "__splink__analyse_blocking_rule")
    res = linker._execute_sql_pipeline([df_concat]).as_record_dict()[0]

    # compare with count from each frame
    sql = f"""
        select count(*) as count
        from __splink__df_concat
        {group_by}
        order by count desc
    """
    linker._enqueue_sql(sql, "__splink__cartesian_product")
    cartesian_count = linker._execute_sql_pipeline([df_concat])
    row_count_df = cartesian_count.as_record_dict()
    cartesian_count.drop_table_from_database_and_remove_from_cache()
    # check this is what we expect from input
    assert frame_sizes == [frame["count"] for frame in row_count_df]

    computed_value_count = calculate_cartesian(row_count_df, link_type)

    assert computed_value_count == res["count_of_pairwise_comparisons_generated"]
