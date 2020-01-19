FROM jupyter/all-spark-notebook:latest

COPY . ${HOME}
USER root

RUN pip install .

RUN chown -R ${NB_UID} ${HOME}
USER ${NB_USER}

