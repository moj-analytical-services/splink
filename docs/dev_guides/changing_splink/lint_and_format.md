## Linting your code

We use [ruff](https://github.com/charliermarsh/ruff) for linting and formatting.

To quickly run both the linter and formatter, you can source the linting bash script (shown below). The *-f* flag can be called to run automatic fixes with ruff.
If you simply wish for ruff to print the errors it finds to the console, remove this flag.

```sh
uv run ruff format
uv run ruff check .
```


## Additional Rules

`ruff` contains an extensive arsenal of [linting rules and techniques](https://beta.ruff.rs/docs/rules/) that can be applied.

If you wish to add an addition rule, do so in the `pyproject.toml` file in the root of the project.