## Linting your code

We use [ruff](https://github.com/charliermarsh/ruff) for linting and formatting.

To run the formatter and then the linter, run the following from the root of the repository. Add `--fix` to the second command to let `ruff` apply automatic fixes; without it, `ruff` only prints the errors it finds to the console.

```sh
uv run ruff format
uv run ruff check .
```

The checks that run on pull requests are also available as `make` targets: `make format-check`, `make lint` and `make typecheck` (`mypy`). `make check` runs all three, and `make format` formats the code and applies the automatic fixes from `ruff`. If you don't have `make` (for example on Windows), run the `uv run ...` commands from the [`Makefile`](https://github.com/moj-analytical-services/splink/blob/master/Makefile) directly.


## Additional Rules

`ruff` contains an extensive arsenal of [linting rules and techniques](https://beta.ruff.rs/docs/rules/) that can be applied.

If you wish to add an addition rule, do so in the `pyproject.toml` file in the root of the project.