import importlib, inspect
from pathlib import Path

from splink.comparison_level import ComparisonLevel
from splink.comparison import Comparison
from splink.dialect_base import DialectBase

# could always pick this up dynamically, but feels like a reasonable upkeep to fix this order
dialects = (
    "spark", "duckdb", "athena", "sqlite"
)

dialect_levels = {}
dialect_comps = {}
for dialect in dialects:
    dialect_levels[dialect] = []
    dialect_comps[dialect] = []
    for class_name, cls in inspect.getmembers(
            importlib.import_module(f".{dialect}_comparison_level_library", package=f"splink.{dialect}"),
            inspect.isclass
        ):
        if issubclass(cls, ComparisonLevel) and issubclass(cls, DialectBase):
            dialect_levels[dialect].append(class_name)
    for class_name, cls in inspect.getmembers(
            importlib.import_module(f".{dialect}_comparison_library", package=f"splink.{dialect}"),
            inspect.isclass
        ):
        if issubclass(cls, Comparison) and issubclass(cls, DialectBase):
            dialect_comps[dialect].append(class_name)

all_sorted_levels = sorted({y for x in dialect_levels.values() for y in x})
all_sorted_comps = sorted({y for x in dialect_comps.values() for y in x})

opts_inv_lev = {lev: [dialect for dialect in dialects if lev in dialect_levels[dialect]] for lev in all_sorted_levels}
opts_inv_comp = {lev: [dialect for dialect in dialects if lev in dialect_comps[dialect]] for lev in all_sorted_comps}

yes_string, no_string = "✓", ""

def table_from_opts(opts):

    table = f"""||{'|'.join(dialects)}|
    |-|-{'|-'.join('' for d in dialects)}|
    """
    for level_name, level_dialects in opts.items():
        table += f"|`{level_name}`|{'|'.join(yes_string if d in level_dialects else no_string for d in dialects)}|\n"
        # table += "---\n"
    return table

cll_table = table_from_opts(opts_inv_lev)
cl_table = table_from_opts(opts_inv_comp)
with open(Path("docs") / "includes" / "comparison_level_library_detailed.md", "w+") as f:
    f.write(cll_table)
with open(Path("docs") / "includes" / "comparison_library_detailed.md", "w+") as f:
    f.write(cl_table)
