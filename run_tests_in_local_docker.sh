docker build -t splink -f Dockerfile_testrunner .
docker run --rm splink pytest -v -s