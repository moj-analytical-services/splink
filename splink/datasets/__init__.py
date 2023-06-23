from dataclasses import asdict, dataclass
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


splink_demo_data_dir = (
    "https://raw.githubusercontent.com/moj-analytical-services/splink_demos/master/data"
)
_datasets = [
    _DataSetMetaData(
        "fake_1000",
        f"{splink_demo_data_dir}/fake_1000.csv",
        "Fake 1000 from splink demos",
    ),
    _DataSetMetaData(
        "fake_20000",
        f"{splink_demo_data_dir}/fake_20000.csv",
        "Fake 20000 from splink demos",
    ),
]


class _SplinkDataSetsMeta(type):
    cache_dir = DATASETDIR / "__splinkdata_cache__"

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

    # TODO: do we want description here?
    @classmethod
    def class_attribute_factory(cls, dataset_name, url, description, data_format):
        def lazyload_data(self):
            file_loc = cls.cache_dir / f"{dataset_name}.{data_format}"
            if not cls.datafile_exists(file_loc):
                urlretrieve(url, file_loc)
                print(f"downloading: {url}")
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

        return property(lazyload_data)

    @staticmethod
    def datafile_exists(file_loc):
        return file_loc.is_file()


class _SplinkDataSets(metaclass=_SplinkDataSetsMeta, datasets=_datasets):
    pass


splink_data_sets = _SplinkDataSets()
