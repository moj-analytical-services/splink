## Creating a Virtual Environment for Splink

### Managing Dependencies with Poetry

Splink utilises `poetry` for managing its core dependencies, offering a clean and effective solution for tracking and resolving any ensuing package and version conflicts.

You can find a list of Splink's core dependencies within the [pyproject.toml](https://github.com/moj-analytical-services/splink/blob/master/pyproject.toml) file.

#### Fundamental Commands in Poetry

Below are some useful commands to help in the maintenance and upkeep of the [pyproject.toml](https://github.com/moj-analytical-services/splink/blob/master/pyproject.toml) file.

**Adding Packages**
- To incorporate a new package into Splink:
  ```sh
  poetry add <package-name>
  ```
- To specify a version when adding a new package:
  ```sh
  poetry add <package-name>==<version>
  # Add quotes if you want to use other equality calls
  poetry add "<package-name> >= <version>"
  ```

**Modifying Packages**
- To remove a package from the project:
  ```sh
  poetry remove <package-name>
  ```
- Updating an existing package to a specific version:
  ```sh
  poetry add <package-name>==<version>
  poetry add "<package-name> >= <version>"
  ```
- To update an existing package to the latest version:
  ```sh
  poetry add <package-name>==<version>
  poetry update <package-name>
  ```
  Note: Direct updates can also be performed within the pyproject.toml file.

**Locking the Project**
- To update the existing `poetry.lock` file, thereby locking the project to ensure consistent dependency installation across different environments:
  ```sh
  poetry lock
  ```
  Note: This should be used sparingly due to our loose dependency requirements and the resulting time to solve the dependency graph. If you only need to update a single dependency, update it using `poetry add <pkg>==<version>` instead.

**Installing Dependencies**
- To install project dependencies as per the lock file:
  ```sh
  poetry install
  ```
- For optional dependencies, additional flags are required. For instance, to install dependencies along with Spark support:
  ```sh
  poetry install -E spark
  ```

A comprehensive list of Poetry commands is available in the [Poetry documentation](https://python-poetry.org/docs/cli/).

### Automating Virtual Environment Creation

To streamline the creation of a virtual environment via `venv`, you may use the [create_venv.sh](https://github.com/moj-analytical-services/splink/blob/master/scripts/create_venv.sh) script.

This script facilitates the automatic setup of a virtual environment, with the default environment name being **venv**.

**Default Environment Creation:**
```sh
source scripts/create_venv.sh
```

**Specifying a Custom Environment Name:**
```sh
source scripts/create_venv.sh <name_of_venv>
```
