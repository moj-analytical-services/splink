# this script to be run at the root of the repo
# otherwise paths for generated files are incorrect
import importlib
import inspect
from pathlib import Path

from splink.datasets import _datasets as datasets_info
from splink.comparison_level import ComparisonLevel
from splink.dialect_base import DialectBase


def make_md_table(datasets):
    # start table with a header row
    table = f"|dataset name|description|rows|unique entities|link to source|\n"
    # and a separator row
    table += f"|-|-|-|-|-|\n"

    # then rows for each level indicating which dialects support it
    for ds in datasets:
        table += f"|`{ds.dataset_name}`"
        table += f"|{ds.description}"
        table += f"|{ds.rows}"
        table += f"|{ds.unique_entities}"
        table += f"|[source]({ds.url})"
        table += "|\n"
    return table


dataset_table = make_md_table(datasets_info)
dataset_table_file = "datasets_table.md"
with open(Path("docs") / "includes" / "generated_files" / dataset_table_file, "w+") as f:
    f.write(dataset_table)