"""
Local module
============

This example demonstrates how local modules can be imported.
This module is imported in the example 'Plotting the exponential function'
(``plot_exp.py``).
"""

## Profile Columns

from splink.datasets import splink_datasets

df = splink_datasets.fake_1000

from splink.duckdb.linker import DuckDBLinker
linker = DuckDBLinker(df)

linker.profile_columns(["first_name", "city", "surname", "email", "substr(dob, 1,4)"], top_n=10, bottom_n=5)

