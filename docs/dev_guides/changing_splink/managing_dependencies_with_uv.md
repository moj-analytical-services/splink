Splink utilises `uv` for managing its core dependencies, offering a clean and effective solution for tracking and resolving any ensuing package and version conflicts.

You can find a list of Splink's core dependencies within the [pyproject.toml](https://github.com/moj-analytical-services/splink/blob/master/pyproject.toml) file.

A comprehensive list of uv commands is available in the [uv documentation](https://docs.astral.sh/uv/).

## Fundamental Commands in uv

Below are some useful commands to help in the maintenance and upkeep of the [pyproject.toml](https://github.com/moj-analytical-services/splink/blob/master/pyproject.toml) file.

### Adding Packages

To incorporate a new package into Splink:
```sh
uv add <package-name>
```

To specify a version when adding a new package:
```sh
uv add <package-name>==<version>
# Add quotes if you want to use other equality calls
uv add "<package-name> >= <version>"
```

For non-dev dependencies, you should generally only use the `>=` form so as not to be overly restrictive for users.

### Modifying Packages

To remove a package from the project:

```sh
uv remove <package-name>
```

Updating an existing package to a specific version:

```sh
uv add <package-name>==<version>
uv add "<package-name> >= <version>"
```

To update an existing package to the latest version leave out the version specification:

```sh
uv add <package-name>
```

Note: Direct updates can also be performed within the pyproject.toml file.

### Locking the Project

To update the existing `uv.lock` file, thereby locking the project to ensure consistent dependency installation across different environments:

```sh
uv lock
```

### Installing Dependencies

To install project dependencies as per the lock file:

```sh
uv sync
```

For optional dependencies, additional flags are required. For instance, to install dependencies along with Spark support:

```sh
uv sync --extra spark
```

To install everything:

```sh
uv sync --group dev --group linting --group testing --group typechecking --group docs --all-extras
```

See [pyproject.toml](https://github.com/moj-analytical-services/splink/blob/master/pyproject.toml) for details of dependency groups and available extras.

### Running commands

You can run commands from your shell with `uv run`, e.g.

```sh
uv run pytest tests
```

You can specify a python version with `--python` or `-p`:

```sh
uv run -p 3.12 pytest tests
```

When using `uv run`, `uv` will automatically sync your project before running the command.
