## Building a virtual environment for Splink

Splink uses `poetry` to track and manage our core dependencies and additionally resolve any package conflicts that these may result in.

A list of the core dependencies used within Splink can be found in [pyproject.toml](https://github.com/moj-analytical-services/splink/blob/master/pyproject.toml).

To automatically create a simple virtual environment using `venv`, simply run [this script](https://github.com/moj-analytical-services/splink/scripts/create_venv.sh):
```sh
source scripts/create_venv.sh
```