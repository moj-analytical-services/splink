# Install
Splink supports python 3.7+.

To obtain the latest released version of splink you can install from PyPI using pip:
```shell
pip install splink
```

or if you prefer, you can instead install splink using conda:
```shell
conda install -c conda-forge splink
```

## DuckDB-less Installation
Should you be unable to install `DuckDB` to your local machine, you can still run `Splink` without the `DuckDB` dependency using a small workaround.

To start, install the latest released version of splink from PyPI without any dependencies using:
```shell
pip install splink --no-deps
```

Then, to install the remaining requirements, download the following `requirements.txt` from our github repository using:
```shell
github_url="https://raw.githubusercontent.com/moj-analytical-services/splink/master/scripts/duckdbless_requirements.txt"
output_file="splink_requirements.txt"

# Download the file from GitHub using curl
curl -o "$output_file" "$github_url"
```

Or, if you're either unable to download it directly from github or you'd rather create the file manually, simply:

1. Create a file called `splink_requirements.txt`
2. Copy and paste the contents from our [duckdbless requirements file](https://github.com/moj-analytical-services/splink/blob/master/scripts/duckdbless_requirements.txt) into your file.

Finally, run the following command within your virtual environment to install the remaining splink dependencies:
```shell
pip install -r splink_requirements.txt
```
