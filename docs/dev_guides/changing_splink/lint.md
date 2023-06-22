## Linting your code

For linting, we currently make use of both [ruff](https://github.com/charliermarsh/ruff) and [black](https://github.com/psf/black).

These are used to ensure we produce readable, maintainable, and more consistent code.

To quickly run both linters, simply run this bash script. The *-f* flag is called to run an automatic fix with ruff.
If you simply wish for ruff to print the errors it finds to the console, remove this flag.

```sh
source scripts/lint_and_format.sh -f  # with the fix flag
```
```sh
source scripts/lint_and_format.sh  # without
```

Alternatively, you can run ruff and black separately from a terminal instance.

For black, you need to run:
`black .`

and ruff requires:
`ruff --fix .` for automatic fixes and error printouts
or `ruff --show-source .` for a more thorough breakdown.