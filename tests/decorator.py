import pytest

dialect_groups = {
    "duckdb": ["default"],
    "spark": [],
    "sqlite": [],
    "postgres": [],
}
for groups in dialect_groups.values():
    groups.append("all")


def invert(sql_dialects_missing):
    return (
        sql_d for sql_d in dialect_groups.keys() if sql_d not in sql_dialects_missing
    )


def mark_with_dialects_excluding(*sql_dialects_missing):
    sql_dialects = invert(sql_dialects_missing)
    return mark_with_dialects_including(*sql_dialects, pass_dialect=True)


def mark_with_dialects_including(*sql_dialects, pass_dialect=False):
    def mark_decorator(test_fn):
        params = []
        all_marks = []
        for sql_d in sql_dialects:
            # marks for whatever groups the dialect is in
            marks = [
                getattr(pytest.mark, dialect_group)
                for dialect_group in dialect_groups[sql_d]
            ]
            # plus the basic dialect mark
            dialect_mark = getattr(pytest.mark, sql_d)
            dialect_only_mark = getattr(pytest.mark, f"{sql_d}_only")
            marks += [dialect_mark, dialect_only_mark]
            params.append(pytest.param(sql_d, marks=marks))
            # will end up with duplicates, but think that's okay. for now at least.
            all_marks += marks

        if pass_dialect:
            test_fn = pytest.mark.parametrize("dialect", params)(test_fn)
        else:
            for mark in all_marks:
                test_fn = mark(test_fn)
        return test_fn

    return mark_decorator
