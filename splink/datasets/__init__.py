from pathlib import Path
from urllib.request import urlretrieve

import pandas as pd

DATASETDIR = Path(__file__).parent

_datasets = {
    "fake_1000": "https://raw.githubusercontent.com/moj-analytical-services/splink_demos/master/data/fake_1000.csv",
    "fake_20000": "https://raw.githubusercontent.com/moj-analytical-services/splink_demos/master/data/fake_20000.csv",
}


class _SplinkDataSetsMeta(type):
    cache_dir = DATASETDIR / "__splinkdata_cache__"

    def __new__(cls, clsname, bases, attrs, datasets_lookup):
        cls.cache_dir.mkdir(exist_ok=True)
        attributes = {}
        repr_text = "splink_data_sets object with datasets:"
        for dataset_name, url in datasets_lookup.items():
            attributes[dataset_name] = cls.class_attribute_factory(dataset_name, url)
            repr_text += f"\n\t{dataset_name}"

        attributes["__repr__"] = lambda self: repr_text
        return super().__new__(cls, clsname, bases, attributes)

    @classmethod
    def class_attribute_factory(cls, dataset_name, url):
        def lazyload_data(self):
            file_loc = cls.cache_dir / f"{dataset_name}.csv"
            if not cls.datafile_exists(file_loc):
                urlretrieve(url, file_loc)
                print(f"downloading: {url}")
            # only for checking
            else:
                print("cached!")

            return pd.read_csv(file_loc)

        return property(lazyload_data)

    @staticmethod
    def datafile_exists(file_loc):
        return file_loc.is_file()


class _SplinkDataSets(metaclass=_SplinkDataSetsMeta, datasets_lookup=_datasets):
    pass


splink_data_sets = _SplinkDataSets()
