# Enter the version of python you wish to test
FROM python:3.9.10-buster

# https://stackoverflow.com/questions/31196567/installing-java-in-docker-image
# Install OpenJDK-11
RUN apt-get update && \
    apt-get install -y openjdk-11-jre-headless && \
    apt-get clean;

# Fix certificate issues
RUN apt-get update && \
    apt-get install ca-certificates-java && \
    apt-get clean && \
    update-ca-certificates -f;

# Setup JAVA_HOME -- useful for docker commandline
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

ARG YOUR_ENV=testing

ENV PYTHONFAULTHANDLER=1 \
  PYTHONUNBUFFERED=1 \
  PYTHONHASHSEED=random \
  PIP_NO_CACHE_DIR=off \
  PIP_DISABLE_PIP_VERSION_CHECK=on \
  PIP_DEFAULT_TIMEOUT=100 \
  POETRY_VERSION=1.3.0

RUN curl -sSL https://install.python-poetry.org | python3 -
COPY ./ ./

# RUN pip install poetry
RUN pip install "poetry==$POETRY_VERSION"
# Project initialization:
RUN poetry config virtualenvs.create false \
  && poetry install $(test "$YOUR_ENV" == production) --no-interaction --no-ansi

RUN pip install pytest
CMD python3 -m pytest tests/ -s --disable-pytest-warnings