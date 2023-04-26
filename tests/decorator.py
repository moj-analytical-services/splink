import pytest


dialect_groups = {
    "duckdb": ["default"],
    "spark": ["default"],
    "sqlite": [],
}
for sql_d, groups in dialect_groups.items():
    groups.append("all")


def invert(sql_dialects_missing):
    return [sql_d for sql_d in dialect_groups.keys() if sql_d not in sql_dialects_missing]


def mark_tests_without(sql_dialects_missing=None):
    if sql_dialects_missing is None:
        sql_dialects_missing = []
    sql_dialects = invert(sql_dialects_missing)
    def mark_decorator(test_fn):
        params = []
        for sql_d in sql_dialects:
            # marks for whatever groups the dialect is in
            marks = [
                getattr(pytest.mark, dialect_group) for dialect_group in dialect_groups[sql_d]
            ]
            # plus the basic dialect mark
            dialect_mark = getattr(pytest.mark, sql_d)
            marks.append(dialect_mark)
            params.append(pytest.param(sql_d, marks=marks))

        test_fn = pytest.mark.parametrize(
            "dialect",
            params
        )(test_fn)
        return test_fn
    return mark_decorator

