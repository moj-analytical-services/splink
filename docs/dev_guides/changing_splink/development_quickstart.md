Splink is a complex project with many dependencies.
This page provides step-by-step instructions for getting set up to develop Splink.
Once you have followed these instructions, you should be all set to start making changes.

## Step 0: Unix-like operating system

We highly recommend developing Splink on a Unix-like operating system, such as MacOS or Linux.
While it is possible to develop on another operating system such as Windows, we do not provide
instructions for how to do so.

**Luckily, Windows users can easily fulfil this requirement by installing the Windows Subsystem for Linux (WSL):**

- Open PowerShell as Administrator: Right-click the Start button, select “Windows Terminal (Admin)”, and ensure PowerShell is the selected shell.
- Run the command `wsl --install`.
- You can find more guidance on setting up WSL [on the Microsoft website](https://learn.microsoft.com/en-us/windows/wsl/install)
  but you don't _need_ to do anything additional.
- Open the Windows Terminal again (does not need to be Admin) and select the Ubuntu shell.
  Follow the rest of these instructions in that shell.

## Step 1: Clone Splink

If you haven't already, create a fork of the Splink repository.
You can find the Splink repository [here](https://github.com/moj-analytical-services/splink),
or [click here](https://github.com/moj-analytical-services/splink/fork) to go directly to making a fork.
Clone **your fork** to whatever directory you want to work in with `git clone https://github.com/<YOUR_USERNAME>/splink.git`.

## Step 2: Choose how to install system dependencies

Developing Splink requires Python, as well as uv (the package manager we use to install Python package dependencies).
Python can be installed using uv, so does not need to be installed independently (although it can be).
Running Spark or PostgreSQL on your computer to test those backends requires additional dependencies.
Athena only runs in the AWS cloud, so to locally run the tests for that backend you will need to create an AWS account and
configure Splink to use it.

There are two ways to install these system dependencies: globally on your computer, or in an isolated conda environment.

The decision of which approach to take is subjective.

If you already have uv installed (plus Java and PostgreSQL if you want to run the
Spark and PostgreSQL backends locally), there is probably little advantage to using `conda`.

On the other hand, `conda` is particularly suitable if:

- You're already a `conda` user, and/or
- You're working in an environment where security policies prevent the installation of system level packages like Java
- You don't want to do global installs of some of the requirements like Java

## Step 3, Manual install option: Install system dependencies

### uv

To install uv follow [the installation guide in their docs](https://docs.astral.sh/uv/getting-started/installation/).

### Python

Once uv is installed, Python can be installed using uv, as outlined in [the uv page on installing python](https://docs.astral.sh/uv/guides/install-python/).

### Java

The instructions to install Java globally depend on your operating system.
Generally, some version of Java will be available from your operating system's
package manager.
Note that you must install a version of Java **earlier than Java 18** because
Splink currently uses an older version of Spark.

As an example, you could run this on Ubuntu:

```sh
sudo apt install openjdk-11-jre-headless
```

### PostgreSQL (optional)

Follow [the instructions on the PostgreSQL website](https://www.postgresql.org/download/)
to install it on your computer.

Then, we will need to set up a database for Splink.
You can achieve that with the following commands:

```sh
initdb splink_db
pg_ctl -D splink_db start --wait -l ./splink_db_log
createdb splink_db # The inner database
psql -d splink_db <<SQL
  CREATE USER splinkognito CREATEDB CREATEROLE password 'splink123!' ;
SQL
```

Most of these commands are one-time setup, but the `pg_ctl -D splink_db start --wait -l ./splink_db_log`
command will need to be run each time you want to start PostgreSQL (after rebooting, for example).

Alternatively, you can run PostgreSQL using [Docker](https://www.docker.com/).
First, install [Docker Desktop](https://www.docker.com/products/docker-desktop/).

Then run the setup script (a thin wrapper around `docker-compose`) each time you want to start your PostgreSQL server:

```bash
./scripts/postgres_docker/setup.sh
```

and the teardown script each time you want to stop it:

```bash
./scripts/postgres_docker/teardown.sh
```

Included in the docker-compose file is a [pgAdmin](https://www.pgadmin.org/) container to allow easy exploration of the database as you work, which can be accessed in-browser on the default port.
The default username is `a@b.com` with password `b`.

## Step 3, Conda install option: Install system dependencies

These instructions are the same no matter what operating system you are using.
As an added benefit, these installations will be specific to the conda environment
you create for Splink, so they will not interfere with other projects.

For convenience, we have created an automatic installation script that will install all dependencies for you.
It will create an isolated [conda environment](https://conda.io/projects/conda/en/latest/user-guide/getting-started.html) called `splink`.

From the directory where you have cloned the Splink repository, simply run:

```sh
./scripts/conda/development_setup_with_conda.sh
```

If you use a shell besides bash, add the `mamba` CLI to your PATH by running `~/miniforge3/bin/mamba init <your_shell>`
-- e.g. `~/miniforge3/bin/mamba init zsh` for zsh.

If you've run this successfully, restart your terminal and skip to the "Step 5: Activating your environment(s)" section.

If you would prefer to manually go through the steps to have a better understanding of what you are installing, continue
to the next section.

### Install Conda itself

First, we need to install a conda CLI.
Any will do, but we recommend [Miniforge](https://github.com/conda-forge/miniforge), which can be installed like so:

```sh
curl -L -O "https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-$(uname)-$(uname -m).sh"
bash Miniforge3-$(uname)-$(uname -m).sh -b
```

Miniforge is great because it defaults to the community-curated conda-forge channel, and it
installs the `mamba` CLI by default, which is generally faster than the `conda` CLI.

Before you'll be able to run the `mamba` command, you need to run `~/miniforge3/bin/mamba init`
for your shell -- e.g. `~/miniforge3/bin/mamba init` for Bash or `~/miniforge3/bin/mamba init zsh` for zsh.

### Install Conda packages

The rest is easy, because all the other dependencies can be installed as conda packages.
Simply run:

```sh
mamba env create -n splink --file ./scripts/conda/development_environment.yaml
```

Now run `mamba activate splink` to enter your newly created conda environment
-- you will need to do this again each time you open a new terminal.
Run the rest of the steps in this guide _inside_ this environment.
`mamba deactivate` leaves the environment.

## Step 4: Python package dependencies

Splink manages the other Python packages it depends on using uv.
Simply run `uv sync` in the Splink directory to install them.
You can find more options for this command (such as how to install
optional dependencies) on the [managing dependencies with uv page](./managing_dependencies_with_uv.md).

## Step 5: Activating your environment(s)

Depending on the options you chose in this document, you now have either:

- **Only** a uv virtual environment.
- **Both** a conda environment and a uv virtual environment.

If you **did** use conda, then each time you open a terminal to develop
Splink, after navigating to the repository directory, run `mamba activate splink`.

In either case, relevant python commands (such as running tests, scripts, or using the REPL) can now be run using `uv run ...`.

## Step 6: Checking that it worked

If you have installed all the dependencies, including PostgreSQL,
you should be able to run the following command without error (will take about 10 minutes):

```sh
uv run pytest tests/
```

This runs all the Splink tests across the default DuckDB and Spark backends.

For a quicker, but less comprehensive check, try testing that you can import Splink without error:

```sh
uv run python -c "import splink"
```

## Step 7: Visual Studio Code (optional)

You're now all set to develop Splink.
If you have a text editor/IDE you are comfortable with for working on Python packages,
you can use that.
If you don't, we recommend Visual Studio Code.
Here are some tips on how to get started:

- [Install Visual Studio Code](https://code.visualstudio.com/)
- If you are using WSL on Windows, install [the WSL extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-wsl).
  You will want to do all development inside a WSL "remote."
- Install [the Python extension](https://marketplace.visualstudio.com/items?itemName=ms-python.python).
- Use the Python extension's [pytest functionality](https://code.visualstudio.com/docs/python/testing) to run the tests within your IDE.
- Use the [interactive window](https://code.visualstudio.com/docs/python/jupyter-support-py) to run code snippets.
