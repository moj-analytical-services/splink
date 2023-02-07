import importlib, inspect
from pathlib import Path

from splink.comparison_level import ComparisonLevel
from splink.dialect_base import DialectBase

# could always pick this up dynamically, but feels like a reasonable upkeep to fix this order
dialects = (
    "spark", "duckdb", "athena", "sqlite"
)

dialect_levels = {}
for dialect in dialects:
    dialect_levels[dialect] = []
    for class_name, cls in inspect.getmembers(
            importlib.import_module(f".{dialect}_comparison_level_library", package=f"splink.{dialect}"),
            inspect.isclass
        ):
        if issubclass(cls, ComparisonLevel) and issubclass(cls, DialectBase):
            dialect_levels[dialect].append(class_name)

all_sorted_levels = sorted({y for x in dialect_levels.values() for y in x})

opts_inv = {lev: [dialect for dialect in dialects if lev in dialect_levels[dialect]] for lev in all_sorted_levels}

yes_string, no_string = "x", ""

table = f"""||{'|'.join(dialects)}|
|-|-{'|-'.join('' for d in dialects)}|
"""
for level_name, level_dialects in opts_inv.items():
    table += f"|{level_name}|{'|'.join(yes_string if d in level_dialects else no_string for d in dialects)}|\n"
    # table += "---\n"

with open(Path("docs") / "includes" / "comparison_level_library_detailed.md", "w+") as f:
    f.write(table)
