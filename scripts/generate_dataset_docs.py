# this script to be run at the root of the repo
# otherwise paths for generated files are incorrect
from pathlib import Path

from splink.internals.datasets import _datasets as datasets_info
from splink.internals.datasets import _labels as dataset_labels_info


def make_dataset_md_table(datasets):
    # start table with a header row
    table = "|dataset name|description|rows|unique entities|link to source|\n"
    # and a separator row
    table += "|-|-|-|-|-|\n"

    # then rows for each level indicating which dialects support it
    for ds in datasets:
        table += f"|`{ds.dataset_name}`"
        table += f"|{ds.description}"
        table += f"|{ds.rows}"
        table += f"|{ds.unique_entities}"
        table += f"|[source]({ds.url})"
        table += "|\n"
    return table


dataset_table = make_dataset_md_table(datasets_info)
dataset_table_file = "datasets_table.md"
with open(
    Path("docs") / "includes" / "generated_files" / dataset_table_file, "w+"
) as f:
    f.write(dataset_table)


def make_dataset_labels_md_table(datasets):
    # start table with a header row
    table = "|dataset name|description|rows|unique entities|link to source|\n"
    # and a separator row
    table += "|-|-|-|-|-|\n"

    # then rows for each level indicating which dialects support it
    for ds in datasets:
        table += f"|`{ds.dataset_name}`"
        table += f"|{ds.description}"
        table += f"|{ds.rows}"
        table += f"|{ds.unique_entities}"
        table += f"|[source]({ds.url})"
        table += "|\n"
    return table


dataset_labels_table = make_dataset_labels_md_table(dataset_labels_info)
dataset_labels_table_file = "dataset_labels_table.md"
with open(
    Path("docs") / "includes" / "generated_files" / dataset_labels_table_file, "w+"
) as f:
    f.write(dataset_labels_table)
