#!/bin/bash

deactivate
python3 -m venv splink-venv
source splink-venv/bin/activate
pip install --upgrade pip
pip install poetry
poetry install
pip install pytest