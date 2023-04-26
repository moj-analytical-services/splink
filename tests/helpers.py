from abc import ABC, abstractmethod

import pandas as pd

import splink.duckdb.duckdb_comparison_level_library as cll_duckdb
import splink.duckdb.duckdb_comparison_library as cl_duckdb
from splink.duckdb.duckdb_linker import DuckDBLinker

class TestHelper(ABC):
    @property
    @abstractmethod
    def linker(self):
        pass

    def extra_linker_args(self, num_frames=1):
        return {}

    @abstractmethod
    def convert_frame(self, df):
        pass

    def load_frame_from_csv(self, path):
        return pd.read_csv(path)

    @property
    @abstractmethod
    def cll(self):
        pass

    @property
    @abstractmethod
    def cl(self):
        pass


class DuckDBTestHelper(TestHelper):
    @property
    def linker(self):
        return DuckDBLinker

    def convert_frame(self, df):
        return df

    @property
    def cll(self):
        return cll_duckdb

    @property
    def cl(self):
        return cl_duckdb

