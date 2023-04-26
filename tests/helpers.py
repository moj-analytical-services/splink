from abc import ABC, abstractmethod

import pandas as pd


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

