import os
import warnings
from dataclasses import asdict, dataclass
from math import floor
from pathlib import Path
from urllib.request import urlretrieve

import pandas as pd

DATASETDIR = Path(__file__).parent

valid_formats = ("csv", "parquet")


@dataclass
class _DataSetMetaData:
    dataset_name: str
    url: str
    description: str = ""
    data_format: str = "csv"

    def __post_init__(self):
        if self.data_format not in valid_formats:
            valid_format_str = "', '".join(valid_formats)
            raise ValueError(
                f"`data_format` must be one of '{valid_format_str}'.  "
                f"Dataset '{self.dataset_name}' labelled with format: "
                f"'{self.data_format}'"
            )


_splink_demo_data_dir = (
    "https://raw.githubusercontent.com/moj-analytical-services/splink_demos/master/data"
)
_datasets = [
    _DataSetMetaData(
        "fake_1000",
        f"{_splink_demo_data_dir}/fake_1000.csv",
        (
            "Fake 1000 from splink demos.  ",
            "Records are 250 simulated people, "
            "with different numbers of duplicates, labelled.",
        ),
    ),
    _DataSetMetaData(
        "fake_20000",
        f"{_splink_demo_data_dir}/fake_20000.csv",
        "Fake 20000 from splink demos",
    ),
]
_cache_dir = DATASETDIR / "__splinkdata_cache__"


class _SplinkDataSetsMeta(type):
    cache_dir = _cache_dir

    def __new__(cls, clsname, bases, attrs, datasets):
        cls.cache_dir.mkdir(exist_ok=True)
        attributes = {}
        repr_text = "splink_data_sets object with datasets:"
        for dataset_meta in datasets:
            dataset_name = dataset_meta.dataset_name
            description = dataset_meta.description

            repr_text += f"\n\t{dataset_name} ({description})"
            attributes[dataset_name] = cls.class_attribute_factory(
                **asdict(dataset_meta)
            )

        attributes["__repr__"] = lambda self: repr_text
        return super().__new__(cls, clsname, bases, attributes)

    @classmethod
    def progress(cls, block_count, block_size, total_size):
        prop_done = (block_count * block_size) / total_size
        if prop_done > 1:
            prop_done = 1
        perc = round(prop_done * 100)
        display = f"\r  download progress: {perc} %\t("
        deciles_done = floor(prop_done * 10)
        for i in range(10):
            if i < deciles_done:
                display += "="
            else:
                display += "."
        display += ")"
        print(display, end="")

    @classmethod
    def class_attribute_factory(cls, dataset_name, url, description, data_format):
        def lazyload_data(self):
            file_loc = cls.cache_dir / f"{dataset_name}.{data_format}"
            if not cls.datafile_exists(file_loc):
                print(f"downloading: {url}")
                urlretrieve(url, file_loc, reporthook=cls.progress)
                print("")
            # only for checking
            else:
                print("cached!")

            if data_format == "csv":
                return pd.read_csv(file_loc)
            if data_format == "parquet":
                return pd.read_parquet(file_loc)
            # just in case we have an invalid format
            raise ValueError(
                f"Error retrieving dataset {dataset_name} - invalid format!"
            )

        # make the docstring be the description
        lazyload_data.__doc__ = description

        return property(lazyload_data)

    @staticmethod
    def datafile_exists(file_loc):
        return file_loc.is_file()


class _SplinkDataUtils:
    def __init__(self):
        pass

    def _list_downloaded_data_files(self):
        return os.listdir(_cache_dir)

    def _trim_suffix(self, filename):
        return filename.split(".")[0]

    def list_downloaded_datasets(self):
        """Return a list of datasets that have already been pre-downloaded
        """
        return [self._trim_suffix(f) for f in self._list_downloaded_data_files()]

    def list_all_datasets(self):
        """Return a list of all available datasets, regardless of whether
        or note they have already been pre-downloaded
        """
        return [d.dataset_name for d in _datasets]

    def show_downloaded_data(self):
        """Print a list of datasets that have already been pre-downloaded
        """
        print(
            "Datasets already downloaded and available:\n"
            + ",\n".join(self.list_downloaded_datasets())
        )

    def clear_downloaded_data(self, datasets: list = None):
        """Delete any pre-downloaded data stored locally.

        Args:
            datasets (list): A list of dataset names (without any file suffix) to delete.
                If `None`, all datasets will be deleted. Default `None`
        """
        available_datasets = self.list_all_datasets()
        if datasets is None:
            datasets = available_datasets
        for ds in datasets:
            if ds not in available_datasets:
                warnings.warn(
                    f"Dataset '{ds}' not recognised, ignoring"
                )
        for f in self._list_downloaded_data_files():
            if self._trim_suffix(f) in datasets:
                os.remove(_cache_dir / f)


class _SplinkDataSets(metaclass=_SplinkDataSetsMeta, datasets=_datasets):
    pass


splink_data_sets = _SplinkDataSets()
splink_data_utils = _SplinkDataUtils()
