import pytest

from splink.internals.dialects import (
    DuckDBDialect,
    PostgresDialect,
    SparkDialect,
    SQLiteDialect,
)


@pytest.mark.parametrize(
    ("dialect", "expected_sql"),
    [
        (DuckDBDialect(), "hash(uid || salt) % 10000"),
        (SparkDialect(), "pmod(hash(uid || salt), 10000)"),
        (
            PostgresDialect(),
            "(((hashtext((uid || salt)::text)) % 10000) + 10000) % 10000",
        ),
        (
            SQLiteDialect(),
            "(((splink_hash(uid || salt)) % 10000) + 10000) % 10000",
        ),
    ],
)
def test_hash_bucket_expression_sql(dialect, expected_sql):
    assert dialect.hash_bucket_expression("uid || salt", 10000) == expected_sql


@pytest.mark.parametrize(
    "dialect",
    [DuckDBDialect(), SparkDialect(), PostgresDialect(), SQLiteDialect()],
)
def test_hash_bucket_expression_rejects_non_positive_modulus(dialect):
    with pytest.raises(ValueError, match="modulus must be positive"):
        dialect.hash_bucket_expression("uid", 0)
