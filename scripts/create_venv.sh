#!/bin/bash

# Deactivate any currently active virtual environment
deactivate 2>/dev/null

# Check if a virtual environment name is provided as an argument
if [ -z "$1" ]; then
  # No name provided, use default name 'venv'
  VENV_NAME="venv"
else
  VENV_NAME=$1
fi

python3 -m venv $VENV_NAME
source $VENV_NAME/bin/activate

pip install --upgrade pip
pip install poetry
# Attempt to install dependencies using poetry
if ! poetry install; then
    echo "Poetry failed to install your packages. You may need to update the poetry.toml "
    echo 'file to use py>=3.8 and then run \`poetry add "pandas>=2.0.0"\` before continuing.'
    exit 1
fi


echo "Virtual environment '$VENV_NAME' set up and activated using poetry."