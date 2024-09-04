from __future__ import annotations

import os
import warnings

from .metadata import dataset_labels, datasets
from .splink_datasets import datasets_cache_dir


class SplinkDataUtils:
    def __init__(self):
        pass

    def _list_downloaded_data_files(self):
        return os.listdir(datasets_cache_dir)

    def _trim_suffix(self, filename):
        return filename.split(".")[0]

    def list_downloaded_datasets(self):
        """Return a list of datasets that have already been pre-downloaded"""
        return [self._trim_suffix(f) for f in self._list_downloaded_data_files()]

    def list_all_datasets(self) -> list[str]:
        """Return a list of all available datasets, regardless of whether
        or not they have already been pre-downloaded
        """
        return [d.dataset_name for d in datasets.values()]

    def list_all_dataset_labels(self) -> list[str]:
        """Return a list of all available dataset labels, regardless of whether
        or not they have already been pre-downloaded
        """
        return [d.dataset_name for d in dataset_labels.values()]

    def show_downloaded_data(self) -> None:
        """Print a list of datasets that have already been pre-downloaded"""
        print(  # noqa: T201
            "Datasets already downloaded and available:\n"
            + ",\n".join(self.list_downloaded_datasets())
        )

    def clear_downloaded_data(self, datasets: list[str] = None) -> None:
        """Delete any pre-downloaded data stored locally.

        Args:
            datasets (list): A list of dataset names (without any file suffix)
                to delete.
                If `None`, all datasets will be deleted. Default `None`
        """
        available_datasets = self.list_all_datasets()
        available_labels = self.list_all_dataset_labels()
        all_available_data = available_datasets + available_labels
        if datasets is None:
            datasets = all_available_data
        for ds in datasets:
            if ds not in all_available_data:
                warnings.warn(
                    f"Dataset '{ds}' not recognised, ignoring",
                    stacklevel=2,
                )
        for f in self._list_downloaded_data_files():
            if self._trim_suffix(f) in datasets:
                os.remove(datasets_cache_dir / f)


splink_dataset_utils = SplinkDataUtils()
