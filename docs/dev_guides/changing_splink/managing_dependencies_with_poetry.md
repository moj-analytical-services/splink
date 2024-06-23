Splink utilises `poetry` for managing its core dependencies, offering a clean and effective solution for tracking and resolving any ensuing package and version conflicts.

You can find a list of Splink's core dependencies within the [pyproject.toml](https://github.com/moj-analytical-services/splink/blob/master/pyproject.toml) file.

A comprehensive list of Poetry commands is available in the [Poetry documentation](https://python-poetry.org/docs/cli/).

## Fundamental Commands in Poetry

Below are some useful commands to help in the maintenance and upkeep of the [pyproject.toml](https://github.com/moj-analytical-services/splink/blob/master/pyproject.toml) file.

### Adding Packages

To incorporate a new package into Splink:
```sh
poetry add <package-name>
```

To specify a version when adding a new package:
```sh
poetry add <package-name>==<version>
# Add quotes if you want to use other equality calls
poetry add "<package-name> >= <version>"
```

### Modifying Packages
To remove a package from the project:

```sh
poetry remove <package-name>
```

Updating an existing package to a specific version:

```sh
poetry add <package-name>==<version>
poetry add "<package-name> >= <version>"
```

To update an existing package to the latest version:

```sh
poetry add <package-name>==<version>
poetry update <package-name>
```

Note: Direct updates can also be performed within the pyproject.toml file.

### Locking the Project
To update the existing `poetry.lock` file, thereby locking the project to ensure consistent dependency installation across different environments:

```sh
poetry lock
```

Note: This updates all dependencies and may take some time. If you only need to update a single dependency, update it using `poetry add <pkg>==<version>` instead.

### Installing Dependencies

To install project dependencies as per the lock file:

```sh
poetry install
```

For optional dependencies, additional flags are required. For instance, to install dependencies along with Spark support:

```sh
poetry install -E spark
```

For optional dependencies, additional flags are required. For instance, to install dependencies along with Spark support:

```sh
poetry install -E spark
```

To install everything:

```sh
poetry install --with dev --with linting --with testing --with benchmarking --with typechecking --with demos --all-extras
```