import splink.comparison_library as cl
import splink.internals.comparison_level_library as cll
from splink import ColumnExpression
from tests.literal_utils import run_comparison_vector_value_tests, run_is_in_level_tests

from .decorator import mark_with_dialects_excluding, mark_with_dialects_including


@mark_with_dialects_excluding()
def test_columns_reversed_level(test_helpers, dialect):
    helper = test_helpers[dialect]
    db_api = helper.extra_linker_args()["db_api"]

    test_cases = [
        {
            "description": "Basic ColumnsReversedLevel, not symmetrical",
            "level": cll.ColumnsReversedLevel("forename", "surname"),
            "inputs": [
                {
                    "forename_l": "John",
                    "forename_r": "Smith",
                    "surname_l": "Smith",
                    "surname_r": "John",
                    "expected": True,
                },
                {
                    "forename_l": "Smith",
                    "forename_r": "John",
                    "surname_l": "John",
                    "surname_r": "Smith",
                    "expected": True,
                },
                {
                    "forename_l": "John",
                    "forename_r": "John",
                    "surname_l": "Smith",
                    "surname_r": "Smith",
                    "expected": False,
                },
            ],
        },
        {
            "description": "ColumnsReversedLevel with symmetrical=True",
            "level": cll.ColumnsReversedLevel("forename", "surname", symmetrical=True),
            "inputs": [
                {
                    "forename_l": "John",
                    "forename_r": "Smith",
                    "surname_l": "Smith",
                    "surname_r": "John",
                    "expected": True,
                },
                {
                    "forename_l": "Smith",
                    "forename_r": "John",
                    "surname_l": "John",
                    "surname_r": "Smith",
                    "expected": True,
                },
                {
                    "forename_l": "John",
                    "forename_r": "John",
                    "surname_l": "Smith",
                    "surname_r": "Smith",
                    "expected": False,
                },
            ],
        },
        {
            "description": "ColumnsReversedLevel with ColumnExpression",
            "level": cll.ColumnsReversedLevel(
                ColumnExpression("forename").lower(),
                ColumnExpression("surname").lower(),
            ),
            "inputs": [
                {
                    "forename_l": "John",
                    "forename_r": "SMITH",
                    "surname_l": "Smith",
                    "surname_r": "JOHN",
                    "expected": True,
                },
                {
                    "forename_l": "JOHN",
                    "forename_r": "smith",
                    "surname_l": "SMITH",
                    "surname_r": "john",
                    "expected": True,
                },
                {
                    "forename_l": "John",
                    "forename_r": "John",
                    "surname_l": "Smith",
                    "surname_r": "Smith",
                    "expected": False,
                },
            ],
        },
    ]

    run_is_in_level_tests(test_cases, db_api)


@mark_with_dialects_excluding()
def test_perc_difference(test_helpers, dialect):
    helper = test_helpers[dialect]
    db_api = helper.extra_linker_args()["db_api"]

    perc_comparison = cl.CustomComparison(
        comparison_description="amount",
        comparison_levels=[
            cll.NullLevel("amount"),
            cll.PercentageDifferenceLevel("amount", 0.0),  # 4
            cll.PercentageDifferenceLevel("amount", (0.2 / 1.2) + 1e-4),  # 3
            cll.PercentageDifferenceLevel("amount", (0.2 / 1.0) + 1e-4),  # 2
            cll.PercentageDifferenceLevel("amount", (60 / 200) + 1e-4),  # 1
            cll.ElseLevel(),
        ],
    )

    test_cases = [
        {
            "comparison": perc_comparison,
            "inputs": [
                {
                    "amount_l": 1.2,
                    "amount_r": 1.0,
                    "expected_value": 3,
                    "expected_label": "Percentage difference of 'amount' within 16.68%",
                },
                {
                    "amount_l": 1.0,
                    "amount_r": 0.8,
                    "expected_value": 2,
                    "expected_label": "Percentage difference of 'amount' within 20.01%",
                },
                {
                    "amount_l": 200.0,
                    "amount_r": 140.0,
                    "expected_value": 1,
                    "expected_label": "Percentage difference of 'amount' within 30.01%",
                },
                {
                    "amount_l": 100.0,
                    "amount_r": 50.0,
                    "expected_value": 0,
                    "expected_label": "All other comparisons",
                },
                {
                    "amount_l": None,
                    "amount_r": 100.0,
                    "expected_value": -1,
                    "expected_label": "amount is NULL",
                },
            ],
        },
    ]

    run_comparison_vector_value_tests(test_cases, db_api)


@mark_with_dialects_excluding()
def test_levenshtein_level(test_helpers, dialect):
    helper = test_helpers[dialect]
    db_api = helper.extra_linker_args()["db_api"]

    levenshtein_comparison = cl.CustomComparison(
        comparison_description="name",
        comparison_levels=[
            cll.NullLevel("name"),
            cll.LevenshteinLevel("name", 0),  # 4
            cll.LevenshteinLevel("name", 1),  # 3
            cll.LevenshteinLevel("name", 2),  # 2
            cll.LevenshteinLevel("name", 3),  # 1
            cll.ElseLevel(),
        ],
    )

    test_cases = [
        {
            "comparison": levenshtein_comparison,
            "inputs": [
                {
                    "name_l": "harry",
                    "name_r": "harry",
                    "expected_value": 4,
                    "expected_label": "Levenshtein distance of name <= 0",
                },
                {
                    "name_l": "harry",
                    "name_r": "barry",
                    "expected_value": 3,
                    "expected_label": "Levenshtein distance of name <= 1",
                },
                {
                    "name_l": "harry",
                    "name_r": "gary",
                    "expected_value": 2,
                    "expected_label": "Levenshtein distance of name <= 2",
                },
                {
                    "name_l": "harry",
                    "name_r": "sally",
                    "expected_value": 1,
                    "expected_label": "Levenshtein distance of name <= 3",
                },
                {
                    "name_l": "harry",
                    "name_r": "harry12345",
                    "expected_value": 0,
                    "expected_label": "All other comparisons",
                },
                {
                    "name_l": None,
                    "name_r": "harry",
                    "expected_value": -1,
                    "expected_label": "name is NULL",
                },
            ],
        },
    ]

    run_comparison_vector_value_tests(test_cases, db_api)


# postgres has no Damerau-Levenshtein
@mark_with_dialects_excluding("postgres")
def test_damerau_levenshtein_level(test_helpers, dialect):
    helper = test_helpers[dialect]
    db_api = helper.extra_linker_args()["db_api"]

    damerau_levenshtein_comparison = cl.CustomComparison(
        comparison_description="name",
        comparison_levels=[
            cll.NullLevel("name"),
            cll.DamerauLevenshteinLevel("name", 0),  # 4
            cll.DamerauLevenshteinLevel("name", 1),  # 3
            cll.DamerauLevenshteinLevel("name", 2),  # 2
            cll.DamerauLevenshteinLevel("name", 3),  # 1
            cll.ElseLevel(),  # 0
        ],
    )

    test_cases = [
        {
            "comparison": damerau_levenshtein_comparison,
            "inputs": [
                {
                    "name_l": "harry",
                    "name_r": "harry",
                    "expected_value": 4,
                    "expected_label": "Damerau-Levenshtein distance of name <= 0",
                },
                {
                    "name_l": "harry",
                    "name_r": "barry",
                    "expected_value": 3,
                    "expected_label": "Damerau-Levenshtein distance of name <= 1",
                },
                {
                    "name_l": "harry",
                    "name_r": "haryr",
                    "expected_value": 3,
                    "expected_label": "Damerau-Levenshtein distance of name <= 1",
                },
                {
                    "name_l": "harry",
                    "name_r": "gary",
                    "expected_value": 2,
                    "expected_label": "Damerau-Levenshtein distance of name <= 2",
                },
                {
                    "name_l": "harry",
                    "name_r": "sally",
                    "expected_value": 1,
                    "expected_label": "Damerau-Levenshtein distance of name <= 3",
                },
                {
                    "name_l": "harry",
                    "name_r": "harry12345",
                    "expected_value": 0,
                    "expected_label": "All other comparisons",
                },
                {
                    "name_l": None,
                    "name_r": "harry",
                    "expected_value": -1,
                    "expected_label": "name is NULL",
                },
            ],
        },
    ]

    run_comparison_vector_value_tests(test_cases, db_api)


@mark_with_dialects_excluding()
def test_absolute_difference(test_helpers, dialect):
    helper = test_helpers[dialect]
    db_api = helper.extra_linker_args()["db_api"]

    abs_comparison = cl.CustomComparison(
        comparison_description="amount",
        comparison_levels=[
            cll.NullLevel("amount"),
            cll.AbsoluteDifferenceLevel("amount", 0),  # 5
            cll.AbsoluteDifferenceLevel("amount", 5),  # 4
            cll.AbsoluteDifferenceLevel("amount", 10),  # 3
            cll.AbsoluteDifferenceLevel("amount", 20),  # 2
            cll.AbsoluteDifferenceLevel("amount", 50),  # 1
            cll.ElseLevel(),
        ],
    )

    test_cases = [
        {
            "comparison": abs_comparison,
            "inputs": [
                {
                    "amount_l": 100,
                    "amount_r": 100,
                    "expected_value": 5,
                    "expected_label": "Absolute difference of 'amount' <= 0",
                },
                {
                    "amount_l": 100,
                    "amount_r": 103,
                    "expected_value": 4,
                    "expected_label": "Absolute difference of 'amount' <= 5",
                },
                {
                    "amount_l": 100,
                    "amount_r": 108,
                    "expected_value": 3,
                    "expected_label": "Absolute difference of 'amount' <= 10",
                },
                {
                    "amount_l": 100,
                    "amount_r": 115,
                    "expected_value": 2,
                    "expected_label": "Absolute difference of 'amount' <= 20",
                },
                {
                    "amount_l": 100,
                    "amount_r": 140,
                    "expected_value": 1,
                    "expected_label": "Absolute difference of 'amount' <= 50",
                },
                {
                    "amount_l": 100,
                    "amount_r": 200,
                    "expected_value": 0,
                    "expected_label": "All other comparisons",
                },
                {
                    "amount_l": None,
                    "amount_r": 100,
                    "expected_value": -1,
                    "expected_label": "amount is NULL",
                },
            ],
        },
    ]

    run_comparison_vector_value_tests(test_cases, db_api)


@mark_with_dialects_including("duckdb", pass_dialect=True)
def test_cosine_similarity_level(test_helpers, dialect):
    import pyarrow as pa

    helper = test_helpers[dialect]
    db_api = helper.extra_linker_args()["db_api"]

    EMBEDDING_DIMENSION = 4

    cosine_similarity_comparison_using_levels = cl.CustomComparison(
        comparison_description="text_vector",
        comparison_levels=[
            cll.NullLevel("text_vector"),
            cll.CosineSimilarityLevel("text_vector", 0.9),  # 3
            cll.CosineSimilarityLevel("text_vector", 0.7),  # 2
            cll.CosineSimilarityLevel("text_vector", 0.5),  # 1
            cll.ElseLevel(),
        ],
    )

    input_dicts = [
        {
            "text_vector_l": [0.5205, 0.4616, 0.3333, 0.2087],
            "text_vector_r": [0.4137, 0.5439, 0.0737, 0.2041],
            "expected_value": 3,
            "expected_label": "Cosine similarity of text_vector >= 0.9",
        },  # Cosine similarity: 0.9312
        {
            "text_vector_l": [0.7026, 0.8887, 0.1711, 0.0525],
            "text_vector_r": [0.4549, 0.4891, 0.1555, 0.6263],
            "expected_value": 2,
            "expected_label": "Cosine similarity of text_vector >= 0.7",
        },  # Cosine similarity: 0.7639
        {
            "text_vector_l": [0.8713, 0.3416, 0.4024, 0.1350],
            "text_vector_r": [0.2104, 0.5763, 0.0442, 0.0872],
            "expected_value": 1,
            "expected_label": "Cosine similarity of text_vector >= 0.5",
        },  # Cosine similarity: 0.6418
        {
            "text_vector_l": [0.99, 0.00, 0.99, 0.00],
            "text_vector_r": [0.00, 0.99, 0.00, 0.99],
            "expected_value": 0,
            "expected_label": "All other comparisons",
        },
        {
            "text_vector_l": None,
            "text_vector_r": [0.99, 0.99, 0.99, 0.99],
            "expected_value": -1,
            "expected_label": "text_vector is NULL",
        },
    ]

    # Convert input_dicts to a pyarrow Table
    inputs_pa = pa.Table.from_pydict(
        {
            "text_vector_l": [d["text_vector_l"] for d in input_dicts],
            "text_vector_r": [d["text_vector_r"] for d in input_dicts],
            "expected_value": [d["expected_value"] for d in input_dicts],
            "expected_label": [d["expected_label"] for d in input_dicts],
        },
        schema=pa.schema(
            [
                ("text_vector_l", pa.list_(pa.float32(), EMBEDDING_DIMENSION)),
                ("text_vector_r", pa.list_(pa.float32(), EMBEDDING_DIMENSION)),
                ("expected_value", pa.int16()),
                ("expected_label", pa.string()),
            ]
        ),
    )

    test_cases = [
        {
            "comparison": cosine_similarity_comparison_using_levels,
            "inputs": inputs_pa,
        },
    ]

    run_comparison_vector_value_tests(test_cases, db_api)

    cosine_similarity_comparison = cl.CosineSimilarityAtThresholds(
        "text_vector", [0.9, 0.7, 0.5]
    )

    test_cases = [
        {
            "comparison": cosine_similarity_comparison,
            "inputs": inputs_pa,
        },
    ]

    run_comparison_vector_value_tests(test_cases, db_api)
