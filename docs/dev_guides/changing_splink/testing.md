---
tags:
  - Testing
  - Pytest
  - Backends
---
# Testing in Splink

Tests in Splink make use of the [pytest](https://docs.pytest.org) framework. You can find the tests themselves in [the tests folder](https://github.com/moj-analytical-services/splink/tree/master/tests).

Splink tests can be broadly categorised into three sets:

* **'Core' tests** - these are tests which test some specific bit of functionality which does not depend on any specific SQL dialect. They are usually unit tests - examples are testing [`InputColumn`](https://github.com/moj-analytical-services/splink/blob/master/tests/test_input_column.py) and testing the [latitude-longitude distance calculation](https://github.com/moj-analytical-services/splink/blob/master/tests/test_lat_long_distance.py).
* **Backend-agnostic tests** - these are tests which run against some SQL backend, but which are written in such a way that they can run against many backends by making use of the [backend-agnostic testing framework](#backend-agnostic-testing). The majority of tests are of this type.
* **Backend-specific tests** - these are tests which run against a specific SQL backend, and test some feature particular to this backend. There are not many of these, as Splink is designed to run very similarly independent of the backend used.

## Running tests

### Running tests locally

To run tests locally against duckdb only (the default) run:
```sh
uv run pytest tests/
```

To run a single test file, append the filename to the `tests/` folder call, for example:

```sh
uv run pytest tests/test_u_train.py
```

or for a single test, additionally append the test name after a pair of colons, as:

```sh
uv run pytest tests/test_u_train.py::test_u_train_multilink
```

??? tip "Further useful pytest options"
    There may be many warnings emitted, for instance by library dependencies, cluttering your output in which case you can use `--disable-pytest-warnings` or `-W ignore` so that these will not be displayed. Some additional command-line options that may be useful:

    * `-s` to disable output capture, so that test output is displayed in the terminal in all cases
    * `-v` for verbose mode, where each test instance will be displayed on a separate line with status
    * `-q` for quiet mode, where output is extremely minimal
    * `-x` to fail on first error/failure rather than continuing to run all selected tests
        *
    * `-m some_mark` run only those tests marked with `some_mark` - see [below](#running-tests-for-specific-backends-or-backend-groups) for useful options here

    For instance usage might be:
    ```sh
    # ignore warnings, display output
    pytest -W ignore -s tests/
    ```

    or
    ```sh
    # ignore warnings, verbose output, fail on first error/failure
    pytest -W ignore -v -x tests/
    ```

    You can find a host of other available options using pytest's in-built help:
    ```sh
    pytest -h
    ```

#### Running tests for specific backends or backend groups

You may wish to run tests relating to to specific backends, tests which are backend-independent, or any combinations of these. Splink allows for various combinations by making use of `pytest`'s [`mark` feature](https://docs.pytest.org/en/latest/example/markers.html).

If when you invoke pytest you pass no marks explicitly, there will be an implicit mark of `default`, as per the [pyproject.toml pytest.ini configuration](https://github.com/moj-analytical-services/splink/blob/master/pyproject.toml), and see also the [decorator.py](https://github.com/moj-analytical-services/splink/blob/master/tests/decorator.py) file.

The available options are:

##### Run core tests
Option for running only the backend-independent 'core' tests:

* `uv run pytest tests/ -m core` - run only the 'core' tests, meaning those without dialect-dependence. In practice this means any test that hasn't been decorated using `mark_with_dialects_excluding` or `mark_with_dialects_including`.

##### Run tests on a specific backend
Options for running tests on one backend only - this includes tests written [specifically for that backend](#backend-specific-tests), as well as [backend-agnostic tests](#backend-agnostic-testing) supported for that backend.

* `uv run pytest tests/ -m duckdb` - run all `duckdb` tests, and all `core` tests
    * & similarly for other dialects
* `uv run pytest tests/ -m duckdb_only` - run all `duckdb` tests only, and _not_ the `core` tests
    * & similarly for other dialects

##### Run tests across multiple backends
Options for running tests on multiple backends (including all backends) - this includes tests written [specifically for those backends](#backend-specific-tests), as well as [backend-agnostic tests](#backend-agnostic-testing) supported for those backends.

 * `pytest tests/ -m default` or equivalently `pytest tests/` - run all tests in the `default` group. The `default` group consists of the `core` tests, and those dialects in the `default` group - currently `spark` and `duckdb`.
    * Other groups of dialects can be added and will similarly run with `pytest tests/ -m new_dialect_group`. Dialects within the current scope of testing and the groups they belong to are defined in the `dialect_groups` dictionary in [tests/decorator.py](https://github.com/moj-analytical-services/splink/blob/master/tests/decorator.py)
* `pytest tests/ -m all` run all tests for all available dialects

These all work alongside all the other pytest options, so for instance to run the tests for training `probability_two_random_records_match` for only `duckdb`, ignoring warnings, with quiet output, and exiting on the first failure/error:
```sh
pytest -W ignore -q -x -m duckdb tests/test_estimate_prob_two_rr_match.py
```

??? tip "Running tests against a specific version of Python"

    Testing Splink against a specific version of Python, especially newer versions not included in our GitHub Actions, is vital for identifying compatibility issues
    early and reviewing errors reported by users.

    If you're a conda user, you can create a isolated environment according to the
    instructions in the [development quickstart](./development_quickstart.md).

    Another method is to utilise docker 🐳.

    A pre-built Dockerfile for running tests against python version 3.9.10 can be located within [scripts/run_tests.Dockerfile](https://github.com/moj-analytical-services/splink/blob/master/scripts/run_tests.Dockerfile).

    To run, simply use the following docker command from within a terminal and the root folder of a Splink clone:
    ```sh
    docker build -t run_tests:testing -f scripts/run_tests.Dockerfile . && docker run --rm --name splink-test run_tests:testing
    ```

    This will both build and run the tests library.

    Feel free to replace `run_tests:testing` with an image name and tag you're happy with.

    Reusing the same image and tag will overwrite your existing image.

    You can also overwrite the default `CMD` if you want a different set of `pytest` command-line options, for example
    ```sh
    docker run --rm --name splink-test run_tests:testing pytest -W ignore -m spark tests/test_u_train.py
    ```

#### Running with a pre-existing Postgres database

If you have a pre-existing Postgres server you wish to use to run the tests against, you will need to specify environment variables for the credentials where they differ from default (in parentheses):

* `SPLINKTEST_PG_USER` (`splinkognito`)
* `SPLINKTEST_PG_PASSWORD` (`splink123!`)
* `SPLINKTEST_PG_HOST` (`localhost`)
* `SPLINKTEST_PG_PORT` (`5432`)
* `SPLINKTEST_PG_DB` (`splink_db`) - tests will not actually run against this, but it is from a connection to this that the temporary test database + user will be created

While care has been taken to ensure that tests are run using minimal permissions, and are cleaned up after, it is probably wise to run tests connected to a non-important database, in case anything goes wrong.
In addition to the [standard privileges for Splink usage](../../topic_guides/splink_fundamentals/backends/postgres.md#permissions), in order to run the tests you will need:

* `CREATE DATABASE` to create a temporary testing database
* `CREATEROLE` to create a temporary user role with limited privileges, which will be actually used for all the SQL execution in the tests

### Tests in CI

Splink utilises [GitHub actions](https://docs.github.com/en/actions) to run tests for each pull request. This consists of a few independent checks:

* The full test suite is run separately against several different python versions
* The [example notebooks](../../demos/examples/examples_index.md) are checked to ensure they run without error
* The [tutorial notebooks](../../demos/tutorials/00_Tutorial_Introduction.ipynb) are checked to ensure they run without error

## Writing tests

### Core tests

Core tests are treated the same way as ordinary pytest tests. Any test is marked as `core` by default, and will only be excluded from being a core test if it is decorated using either:

* `@mark_with_dialects_excluding` for [backend-agnostic tests](#backend-agnostic-testing), or
* `@mark_with_dialects_including` for [backend-specific tests](#backend-specific-tests)

from the [test decorator file](https://github.com/moj-analytical-services/splink/blob/master/tests/decorator.py).

### Backend-agnostic testing

The majority of tests should be written using the backend-agnostic testing framework. This just provides some small tools which allow tests to be written in a backend-independent way. This means the tests can then by run against _all_ available SQL backends (or a subset, if some lack _necessary_ features for the test).

As an example, let's consider a test that will run on all dialects, and then break down the various parts to see what each is doing.

```py linenums="1"
from tests.decorator import mark_with_dialects_excluding

@mark_with_dialects_excluding()
def test_feature_that_works_for_all_backends(test_helpers, dialect, some_other_test_fixture):
    helper = test_helpers[dialect]

    df = helper.load_frame_from_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
    settings = SettingsCreator(
        link_type="dedupe_only",
        comparisons=[
            cl.ExactMatch("first_name"),
            cl.ExactMatch("surname"),
        ],
        blocking_rules_to_generate_predictions=[
            block_on("first_name"),
        ],
    )
    linker = helper.Linker(
        df,
        settings,
        **helper.extra_linker_args(),
    )


    # and then some actual testing logic
```

Firstly you should import the decorator-factory `mark_with_dialects_excluding`, which will decorate each test function:

```py linenums="1"
from tests.decorator import mark_with_dialects_excluding
```

Then we define the function, and pass parameters:

```py linenums="3" hl_lines="1"
@mark_with_dialects_excluding()
def test_feature_that_works_for_all_backends(test_helpers, dialect, some_other_test_fixture):
```

The decorator `@mark_with_dialects_excluding()` will do two things:

* marks the test it decorates with the appropriate custom `pytest` marks. This ensures that it will be run with tests for each dialect, excluding any that are passed as arguments; in this case it will be run for all dialects, as we have passed no arguments.
* parameterises the test with a string parameter `dialect`, which will be used to configure the test for that dialect. The test will run for each value of `dialect` possible, excluding any passed to the decorator (none in this case).

You should aim to exclude as _few_ dialects as possible - consider if you really need to exclude any. Dialects should only be excluded if the test doesn't make sense for them due to features they lack. The default choice should be the decorator with no arguments `@mark_with_dialects_excluding()`, meaning the test runs for _all_ dialects.

```py linenums="3" hl_lines="2"
@mark_with_dialects_excluding()
def test_feature_that_works_for_all_backends(test_helpers, dialect, some_other_test_fixture):
```

As well as the parameter `dialect` (which is provided by the decorator), we must also pass the helper-factory fixture `test_helpers`. We can additionally pass further [fixtures](https://docs.pytest.org/en/latest/how-to/fixtures.html) if needed - in this case `some_other_test_fixture`.
We could similarly provide an _explicit_ parameterisation to the test, in which case we would also pass these parameters - see [the pytest docs on parameterisation](https://doc.pytest.org/en/latest/example/parametrize.html#set-marks-or-test-id-for-individual-parametrized-test) for more information.


```py linenums="5"
    helper = test_helpers[dialect]
```

The fixture `test_helpers` is simply a dictionary of the specific-dialect test helpers - here we pick the appropriate one for our test.

Each helper has the same set of methods and properties, which encapsulate _all_ of the dialect-dependencies. You can find the full set of properties and methods by examining the source for the [base class `TestHelper`](https://github.com/moj-analytical-services/splink/blob/master/tests/helpers.py).

```py linenums="7"
    df = helper.load_frame_from_csv("./tests/datasets/fake_1000_from_splink_demos.csv")
```

Here we are now actually using a method of the test helper - in this case we are loading a table from a csv to the database and returning it in a form suitable for passing to a Splink linker.

Finally we instantiate the linker, passing any default set of extra arguments provided by the helper, which some dialects require.
```py linenums="18"
    linker = helper.Linker(df, settings_dict, **helper.extra_linker_args())
```


From this point onwards we will be working with the instantiated `linker`, and so will not need to refer to `helper` any more - the rest of the test can be written as usual.

#### Excluding some backends

Now let's consider an example in which we wanted to test a `ComparisonLevel` that included the `split_part` function which does not exist in the `sqlite` dialect. We assume that this particular comparison level is crucial for the test to make sense, otherwise we would rewrite this line to make it run universally. When you come to [run the tests](#running-tests-locally), this test will not run on the `sqlite` backend.

```
{
    "sql_condition": "split_part(email_l, '@', 1) = split_part(email_r, '@', 1)",
    "label_for_charts": "email local-part matches",
}
```



!!! warning Reminder
    Tests should be made available to the widest range of backends possible. Only exclude backends if features not shared by all backends are crucial to the test-logic - otherwise consider rewriting things so that all backends are covered.

We therefore want to exclude `sqlite` backend, as the test relies on features not directly available for that backend, which we can do as follows:

```py linenums="1"
from tests.decorator import mark_with_dialects_excluding

@mark_with_dialects_excluding("sqlite")
def test_feature_that_doesnt_work_with_sqlite(test_helpers, dialect, some_other_test_fixture):
    helper = test_helpers[dialect]

    df = helper.load_frame_from_csv("./tests/datasets/fake_1000_from_splink_demos.csv")

    # and then some actual testing logic
```

The key difference is the argument we pass to the decorator:
```py linenums="3" hl_lines="1"
@mark_with_dialects_excluding("sqlite")
def test_feature_that_doesnt_work_with_sqlite(test_helpers, dialect, some_other_test_fixture):
```
As above this marks the test it decorates with the appropriate custom `pytest` marks, but in this case it ensures that it will be run with tests for each dialect **excluding `sqlite`**. Again `dialect` is passed as a parameter, and the test will run in turn for each value of `dialect` **except for `sqlite`**.


If you need to exclude _multiple_ dialects this is also possible - just pass each as an argument. For example, to decorate a test that is not supported on `spark` _or_ `sqlite`, use the decorator `@mark_with_dialects_excluding("sqlite", "spark")`.

### Backend-specific tests

If you intend to write a test for a specific backend, first consider whether it is definitely specific to that backend - if not then a [backend-agnostic test](#backend-agnostic-testing) would be preferable, as then your test will be run against _many_ backends.
If you really do need to test features peculiar to one backend, then you can write it simply as you would an ordinary `pytest` test. The only difference is that you should decorate it with `@mark_with_dialects_including` (from [tests/decorator.py](https://github.com/moj-analytical-services/splink/blob/master/tests/decorator.py)) - for example:

=== ":simple-duckdb: DuckDB"
    ```py
    @mark_with_dialects_including("duckdb")
    def test_some_specific_duckdb_feature():
        ...
    ```
=== ":simple-apachespark: Spark"
    ```py
    @mark_with_dialects_including("spark")
    def test_some_specific_spark_feature():
        ...
    ```
=== ":simple-sqlite: SQLite"
    ```py
    @mark_with_dialects_including("sqlite")
    def test_some_specific_sqlite_feature():
        ...
    ```

This ensures that the test gets marked appropriately for running when the `Spark` tests should be run, and excludes it from the set of `core` tests.

Note that unlike the exclusive `mark_with_dialects_excluding`, this decorator will _not_ parameterise the test with the `dialect` argument. This is because usage of the _inclusive_ form is largely designed for single-dialect tests. If you wish to override this behaviour and parameterise the test you can use the argument `pass_dialect`, for example `@mark_with_dialects_including("spark", "sqlite", pass_dialect=True)`, in which case you would need to write the test in a [backend-independent manner](#backend-agnostic-testing).
