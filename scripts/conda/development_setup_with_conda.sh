#! /bin/bash

cd "$(dirname "$0")"

set -e
set -x

if ! command -v mamba &> /dev/null
then
    curl -L -O "https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-$(uname)-$(uname -m).sh"
    bash Miniforge3-$(uname)-$(uname -m).sh -b
    mamba config --set auto_activate_base false
    rm ./Miniforge3-$(uname)-$(uname -m).sh
fi

# NOTE: Unlike poetry, conda doesn't have great support for locking dependencies.
# There is conda-lock, but it is pretty experimental and has a bug that makes
# it unusable for Splink as of 3/19/2024: https://github.com/conda/conda-lock/issues/619
mamba env create -n splink --file development_environment.yaml --force
# For informational purposes only, we can track the exact dependency versions in git.
# However, this is platform-specific and will only be added/updated when people run this
# script on a specific platform.
mamba list --explicit --name splink > development_environment_lock_$(uname)-$(uname -m).txt

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
mamba run --no-capture -n splink poetry install --with typechecking --all-extras