# this script to be run at the root of the repo
# otherwise paths for generated files are incorrect
import importlib
import inspect
from pathlib import Path

from splink.comparison import Comparison
from splink.comparison_level import ComparisonLevel
from splink.dialect_base import DialectBase

# could always pick this up dynamically,
# but this way we get to fix the order, and feels like not unreasonable upkeep
dialects = ("duckdb", "spark", "athena", "sqlite", "postgres")
dialect_table_headers = (
    ":simple-duckdb: <br> DuckDB",
    ":simple-apachespark: <br> Spark",
    ":simple-amazonaws: <br> Athena",
    ":simple-sqlite: <br> SQLite",
    ":simple-postgresql: <br> PostgreSql",
)

dialect_levels = {}
dialect_comparisons = {}
dialect_comparison_templates = {}
dialect_level_compositions = {}
for dialect in dialects:
    dialect_levels[dialect] = []
    dialect_comparisons[dialect] = []
    dialect_comparison_templates[dialect] = []
    dialect_level_compositions[dialect] = []
    for class_name, cls in inspect.getmembers(
        importlib.import_module(
            ".comparison_level_library", package=f"splink.{dialect}"
        ),
        inspect.isclass,
    ):
        if issubclass(cls, ComparisonLevel) and issubclass(cls, DialectBase):
            dialect_levels[dialect].append(class_name)
    for class_name, cls in inspect.getmembers(
        importlib.import_module(".comparison_library", package=f"splink.{dialect}"),
        inspect.isclass,
    ):
        if issubclass(cls, Comparison) and issubclass(cls, DialectBase):
            dialect_comparisons[dialect].append(class_name)
    for class_name, cls in inspect.getmembers(
        importlib.import_module(
            ".comparison_template_library", package=f"splink.{dialect}"
        ),
        inspect.isclass,
    ):
        if issubclass(cls, Comparison) and issubclass(cls, DialectBase):
            dialect_comparison_templates[dialect].append(class_name)
    for class_name, cls in inspect.getmembers(
        importlib.import_module(
            ".comparison_level_library", package=f"splink.{dialect}"
        ),
        inspect.isfunction,
    ):
        dialect_level_compositions[dialect].append(class_name)


all_sorted_levels = sorted({y for x in dialect_levels.values() for y in x})
all_sorted_comparisons = sorted({y for x in dialect_comparisons.values() for y in x})
all_sorted_comparison_templates = sorted(
    {y for x in dialect_comparison_templates.values() for y in x}
)
all_sorted_level_compositions = sorted(
    {y for x in dialect_level_compositions.values() for y in x}
)


def base_function_string(input_string):
    # Split the input string by underscores
    words = input_string.split("_")

    # Capitalize the first letter of each word and join them
    transformed_string = "".join(word.capitalize() for word in words)

    # Add the suffix "Base"
    transformed_string += "Base"

    return transformed_string


level_dialects = {
    f"[{lev}](#splink.comparison_level_library.{base_function_string(lev)})": [
        dialect for dialect in dialects if lev in dialect_levels[dialect]
    ]
    for lev in all_sorted_levels
}
comparison_dialects = {
    f"[{comp}](#splink.comparison_library.{base_function_string(comp)})": [
        dialect for dialect in dialects if comp in dialect_comparisons[dialect]
    ]
    for comp in all_sorted_comparisons
}
comparison_template_dialects = {
    f"[{comp_temp}](#splink.comparison_template_library."
    + f"{base_function_string(comp_temp)})": [
        dialect
        for dialect in dialects
        if comp_temp in dialect_comparison_templates[dialect]
    ]
    for comp_temp in all_sorted_comparison_templates
}
level_composition_dialects = {
    f"[{lev_comp}](#splink.comparison_level_composition.{lev_comp})": [
        dialect
        for dialect in dialects
        if lev_comp in dialect_level_compositions[dialect]
    ]
    for lev_comp in all_sorted_level_compositions
}

# strings to use in md table for whether function appears in dialect or not
yes_string, no_string = "✓", ""


def make_md_table(opts):
    # start table with a header row of blank column + all dialects
    table = f"||{'|'.join(dialect_table_headers)}|\n"
    # and a separator row
    table += f"|:-:|:-:{'|:-:'.join('' for d in dialects)}|\n"

    # then rows for each level indicating which dialects support it
    for level_name, level_dialects in opts.items():
        level_yes_no_dialect_string = (
            yes_string if d in level_dialects else no_string for d in dialects
        )
        table += f"|{level_name}|"
        table += f"{'|'.join(level_yes_no_dialect_string)}|\n"
    return table


cll_table = make_md_table(level_dialects)
cll_table_file = "comparison_level_library_dialect_table.md"
cl_table = make_md_table(comparison_dialects)
cl_table_file = "comparison_library_dialect_table.md"
ctl_table = make_md_table(comparison_template_dialects)
ctl_table_file = "comparison_template_library_dialect_table.md"
ccl_table = make_md_table(level_composition_dialects)
ccl_table_file = "comparison_composition_library_dialect_table.md"

with open(Path("docs") / "includes" / "generated_files" / cll_table_file, "w+") as f:
    f.write(cll_table)
with open(Path("docs") / "includes" / "generated_files" / cl_table_file, "w+") as f:
    f.write(cl_table)
with open(Path("docs") / "includes" / "generated_files" / ctl_table_file, "w+") as f:
    f.write(ctl_table)
with open(Path("docs") / "includes" / "generated_files" / ccl_table_file, "w+") as f:
    f.write(ccl_table)
