FROM jupyter/all-spark-notebook:latest

COPY . /

RUN pip install .