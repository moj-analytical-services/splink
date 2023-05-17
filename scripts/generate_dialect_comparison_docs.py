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
dialects = ("duckdb", "spark", "athena", "sqlite")

dialect_levels = {}
dialect_comparisons = {}
dialect_comparison_templates = {}
for dialect in dialects:
    dialect_levels[dialect] = []
    dialect_comparisons[dialect] = []
    dialect_comparison_templates[dialect] = []
    for class_name, cls in inspect.getmembers(
        importlib.import_module(
            f".{dialect}_comparison_level_library", package=f"splink.{dialect}"
        ),
        inspect.isclass,
    ):
        if issubclass(cls, ComparisonLevel) and issubclass(cls, DialectBase):
            dialect_levels[dialect].append(class_name)
    for class_name, cls in inspect.getmembers(
        importlib.import_module(
            f".{dialect}_comparison_library", package=f"splink.{dialect}"
        ),
        inspect.isclass,
    ):
        if issubclass(cls, Comparison) and issubclass(cls, DialectBase):
            dialect_comparisons[dialect].append(class_name)
    for class_name, cls in inspect.getmembers(
        importlib.import_module(
            f".{dialect}_comparison_template_library", package=f"splink.{dialect}"
        ),
        inspect.isclass,
    ):
        if issubclass(cls, Comparison) and issubclass(cls, DialectBase):
            dialect_comparison_templates[dialect].append(class_name)

all_sorted_levels = sorted({y for x in dialect_levels.values() for y in x})
all_sorted_comparisons = sorted({y for x in dialect_comparisons.values() for y in x})
all_sorted_comparison_templates = sorted(
    {y for x in dialect_comparison_templates.values() for y in x}
)

level_dialects = {
    lev: [dialect for dialect in dialects if lev in dialect_levels[dialect]]
    for lev in all_sorted_levels
}
comparison_dialects = {
    comp: [dialect for dialect in dialects if comp in dialect_comparisons[dialect]]
    for comp in all_sorted_comparisons
}
comparison_template_dialects = {
    comp_temp: [
        dialect
        for dialect in dialects
        if comp_temp in dialect_comparison_templates[dialect]
    ]
    for comp_temp in all_sorted_comparison_templates
}

# strings to use in md table for whether function appears in dialect or not
yes_string, no_string = "✓", ""


def make_md_table(opts):
    # start table with a header row of blank column + all dialects
    table = f"||{'|'.join(dialects)}|\n"
    # and a separator row
    table += f"|-|-{'|-'.join('' for d in dialects)}|\n"

    # then rows for each level indicating which dialects support it
    for level_name, level_dialects in opts.items():
        level_yes_no_dialect_string = (
            yes_string if d in level_dialects else no_string for d in dialects
        )
        table += f"|`{level_name}`|"
        table += f"{'|'.join(level_yes_no_dialect_string)}|\n"
    return table


cll_table = make_md_table(level_dialects)
cll_table_file = "comparison_level_library_dialect_table.md"
cl_table = make_md_table(comparison_dialects)
cl_table_file = "comparison_library_dialect_table.md"
ctl_table = make_md_table(comparison_template_dialects)
ctl_table_file = "comparison_template_library_dialect_table.md"

with open(Path("docs") / "includes" / "generated_files" / cll_table_file, "w+") as f:
    f.write(cll_table)
with open(Path("docs") / "includes" / "generated_files" / cl_table_file, "w+") as f:
    f.write(cl_table)
with open(Path("docs") / "includes" / "generated_files" / ctl_table_file, "w+") as f:
    f.write(ctl_table)
