docker build -t sparklink -f Dockerfile_testrunner .
docker run --rm sparklink pytest -v -s