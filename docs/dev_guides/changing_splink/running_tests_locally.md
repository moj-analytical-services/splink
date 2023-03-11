## Running tests locally

To run tests locally, simply run:
```sh
python3 -m pytest tests/ -s --disable-pytest-warnings
```

To run a single test, append the filename to the `tests/` folder call:
```sh
python3 -m pytest tests/my_test_file.py -s --disable-pytest-warnings
```

## Running tests with docker 🐳

If you want to test Splink against a specific version of python, the easiest method is to utilise docker 🐳.

Docker allows you to more quickly and easily install a specific version of python and run the existing test library against it.

This is particularly useful if you're using py > 3.9.10 (which is currently in use in our tests github action) and need to run a secondary set of tests.

A pre-built Dockerfile for running tests against python version 3.9.10 can be located within [scripts/run_tests.Dockerfile](https://github.com/moj-analytical-services/splink/scripts/scripts/run_tests.Dockerfile).

To run, simply use the following docker command from within a terminal and the root folder of a splink clone:
```sh
docker build -t run_tests:testing -f scripts/run_tests.Dockerfile . && docker run run_tests:testing
```

This will both build and run the tests library.

Feel free to replace `run_tests:testing` with an image name and tag you're happy with.

Reusing the same image and tag will overwrite your existing image.