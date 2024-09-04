from __future__ import annotations

import io
from pathlib import Path
from urllib.request import urlopen

import pandas as pd

from .metadata import dataset_labels, datasets

DATASETDIR = Path(__file__).parent

datasets_cache_dir = DATASETDIR / "__splinkdata_cache__"


def datafile_exists(file_loc):
    return file_loc.is_file()


def dataset_property(metadata_method):
    ds_meta = metadata_method(None)
    dataset_name = ds_meta.dataset_name
    url = ds_meta.url
    data_format = ds_meta.data_format

    def lazyload_data(self):
        if dataset_name in self._in_memory_data:
            return self._in_memory_data[dataset_name]

        file_loc = datasets_cache_dir / f"{dataset_name}.{data_format}"
        data_source: Path | io.BytesIO
        if not datafile_exists(file_loc):
            print(f"downloading: {url}")  # noqa: T201
            data = urlopen(url)
            print("")  # noqa: T201

            data_source = io.BytesIO(data.read())
            try:
                datasets_cache_dir.mkdir(exist_ok=True)
                with open(file_loc, "wb+") as write_file:
                    write_file.write(data_source.getvalue())
            except PermissionError:
                pass
            data_source.seek(0)
        else:
            data_source = file_loc

        read_function = {
            "csv": pd.read_csv,
            "parquet": pd.read_parquet,
        }.get(data_format, None)

        # just in case we have an invalid format
        if read_function is None:
            raise ValueError(
                f"Error retrieving dataset {dataset_name} - invalid format!"
            )
        df = read_function(data_source)
        self._in_memory_data[dataset_name] = df
        return df

    return lazyload_data


class SplinkDataSets:
    def __init__(self):
        self._in_memory_data = {}

    @property
    @dataset_property
    def fake_1000(self):
        """
        Fake 1000 from splink demos.
        Records are 250 simulated people, with different numbers of duplicates, labelled.

        Columns:
        unique_id, first_name, surname, dob, city, email, cluster
        """  # NOQA: E501
        return datasets["fake_1000"]

    @property
    @dataset_property
    def historical_50k(self):
        """
        The data is based on historical persons scraped from wikidata.
        Duplicate records are introduced with a variety of errors.

        Columns:
        unique_id, cluster, full_name, first_and_surname, first_name, surname, dob, birth_place, postcode_fake, gender, occupation
        """  # NOQA: E501
        return datasets["historical_50k"]

    @property
    @dataset_property
    def febrl3(self):
        """
        The Freely Extensible Biomedical Record Linkage (FEBRL) datasets consist
        of comparison patterns from an epidemiological cancer study in Germany.
        FEBRL3 data set contains 5000 records (2000 originals and 3000 duplicates),
        with a maximum of 5 duplicates based on one original record.

        Columns:
        rec_id, given_name, surname, street_number, address_1, address_2, suburb, postcode, state, date_of_birth, soc_sec_id
        """  # NOQA: E501
        return datasets["febrl3"]

    @property
    @dataset_property
    def febrl4a(self):
        """
        The Freely Extensible Biomedical Record Linkage (FEBRL) datasets consist
        of comparison patterns from an epidemiological cancer study in Germany.
        FEBRL4a contains 5000 original records.

        Columns:
        rec_id, given_name, surname, street_number, address_1, address_2, suburb, postcode, state, date_of_birth, soc_sec_id
        """  # NOQA: E501
        return datasets["febrl4a"]

    @property
    @dataset_property
    def febrl4b(self):
        """
        The Freely Extensible Biomedical Record Linkage (FEBRL) datasets consist
        of comparison patterns from an epidemiological cancer study in Germany.
        FEBRL4b contains 5000 duplicate records, one for each record in FEBRL4a.

        Columns:
        rec_id, given_name, surname, street_number, address_1, address_2, suburb, postcode, state, date_of_birth, soc_sec_id
        """  # NOQA: E501
        return datasets["febrl4b"]

    @property
    @dataset_property
    def transactions_origin(self):
        """
        This data has been generated to resemble bank transactions leaving an
        account. There are no duplicates within the dataset and each transaction
        is designed to have a counterpart arriving in 'transactions_destination'.
        Memo is sometimes truncated or missing."

        Columns:
        ground_truth, memo, transaction_date, amount, unique_id
        """
        return datasets["transactions_origin"]

    @property
    @dataset_property
    def transactions_destination(self):
        """
        This data has been generated to resemble bank transactions leaving an
        account. There are no duplicates within the dataset and each transaction
        is designed to have a counterpart sent from 'transactions_origin'.
        There may be a delay between the source and destination account,
        and the amount may vary due to hidden fees and foreign exchange rates.
        Memo is sometimes truncated or missing.

        Columns:
        ground_truth, memo, transaction_date, amount, unique_id
        """
        return datasets["transactions_destination"]


class SplinkDataSetLabels:
    def __init__(self):
        self._in_memory_data = {}

    @property
    @dataset_property
    def fake_1000_labels(self):
        """
        Clerical labels for fake_1000.

        Columns:
        unique_id_l, source_dataset_l, unique_id_r, source_dataset_r, clerical_match_score
        """  # NOQA: E501
        return dataset_labels["fake_1000"]


# these two singleton objects are the only user-facing portion:
splink_datasets = SplinkDataSets()
splink_dataset_labels = SplinkDataSetLabels()
