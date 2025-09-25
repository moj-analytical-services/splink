#! /bin/bash

cd "$(dirname "$0")"

set -e
set -x

# Is mamba already available in an *interactive* shell?
if ! $SHELL -ic 'command -v mamba &> /dev/null'
then
    # Instructions found here: https://github.com/conda-forge/miniforge?tab=readme-ov-file#downloading-the-installer-as-part-of-a-ci-pipeline
    curl -L -O "https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-$(uname)-$(uname -m).sh"
    bash Miniforge3-$(uname)-$(uname -m).sh -b -p $HOME/miniforge3
    source "${HOME}/miniforge3/etc/profile.d/conda.sh"
    source "${HOME}/miniforge3/etc/profile.d/mamba.sh"
    conda config --set auto_activate_base false
    mamba init $(basename $SHELL)
    rm ./Miniforge3-$(uname)-$(uname -m).sh
else
    # Get location of conda installation used by default in an interactive shell,
    # see https://github.com/conda/conda/issues/7980#issuecomment-472648567
    CONDA_BASE=$($SHELL -ic 'conda info --base')
    source "${CONDA_BASE}/etc/profile.d/conda.sh"
    source "${CONDA_BASE}/etc/profile.d/mamba.sh"
fi

# NOTE: Unlike uv, conda doesn't have great support for locking dependencies.
# There is conda-lock, but it is pretty experimental and has a bug that makes
# it unusable for Splink as of 3/19/2024: https://github.com/conda/conda-lock/issues/619
mamba env create -n splink --file development_environment.yaml --force
# For informational purposes only, we can track the exact dependency versions in git.
# However, this is platform-specific and will only be added/updated when people run this
# script on a specific platform.
mamba list --explicit --name splink > development_environment_lock_$(uname)-$(uname -m).txt

# https://github.com/python-poetry/poetry/issues/4055#issuecomment-2206673892
# TODO: do we need an equivalent setting in uv?

cd ../..
pkill -f "postgres -D splink_db" || true
rm -rf ./splink_db ./splink_db_log
mamba run --no-capture -n splink initdb splink_db
mamba run --no-capture -n splink pg_ctl -D splink_db start --wait -l ./splink_db_log
mamba run --no-capture -n splink createdb splink_db # The inner database
mamba run --no-capture -n splink psql -d splink_db <<SQL
 CREATE USER splinkognito CREATEDB CREATEROLE password 'splink123!' ;
SQL

# Fully reset, since poetry itself may have updated and we have a lockfile
rm -rf ./.venv
mamba run --no-capture -n splink uv sync --all-extras
