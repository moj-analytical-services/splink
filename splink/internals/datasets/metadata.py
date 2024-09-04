from dataclasses import dataclass

_valid_formats = ("csv", "parquet")


@dataclass
class _DataSetMetaData:
    dataset_name: str
    url: str
    rows: str
    unique_entities: str
    description: str = ""
    data_format: str = "csv"

    def __post_init__(self):
        if self.data_format not in _valid_formats:
            valid_format_str = "', '".join(_valid_formats)
            raise ValueError(
                f"`data_format` must be one of '{valid_format_str}'.  "
                f"Dataset '{self.dataset_name}' labelled with format: "
                f"'{self.data_format}'"
            )


_splink_datasets_data_dir = (
    "https://raw.githubusercontent.com/"
    + "moj-analytical-services"
    + "/splink_datasets/master/data"
)
_ds_fake_1000 = _DataSetMetaData(
    "fake_1000",
    f"{_splink_datasets_data_dir}/fake_1000.csv",
    "1,000",
    "250",
    (
        "Fake 1000 from splink demos.  "
        "Records are 250 simulated people, "
        "with different numbers of duplicates, labelled."
    ),
)
_ds_historical_50k = _DataSetMetaData(
    "historical_50k",
    f"{_splink_datasets_data_dir}/historical_figures_with_errors_50k.parquet",
    "50,000",
    "5,156",
    (
        "The data is based on historical persons scraped from wikidata. "
        "Duplicate records are introduced with a variety of errors."
    ),
    "parquet",
)
_ds_febrl3 = _DataSetMetaData(
    "febrl3",
    f"{_splink_datasets_data_dir}/febrl/dataset3.csv",
    "5,000",
    "2,000",
    (
        "The Freely Extensible Biomedical Record Linkage (FEBRL) datasets consist"
        " of comparison patterns from an epidemiological cancer study in Germany."
        "FEBRL3 data set contains 5000 records (2000 originals and 3000 duplicates"
        "), with a maximum of 5 duplicates based on one original record."
    ),
)
_ds_febrl4a = _DataSetMetaData(
    "febrl4a",
    f"{_splink_datasets_data_dir}/febrl/dataset4a.csv",
    "5,000",
    "5,000",
    (
        "The Freely Extensible Biomedical Record Linkage (FEBRL) datasets consist"
        " of comparison patterns from an epidemiological cancer study in Germany."
        "FEBRL4a contains 5000 original records."
    ),
)
_ds_febrl4b = _DataSetMetaData(
    "febrl4b",
    f"{_splink_datasets_data_dir}/febrl/dataset4b.csv",
    "5,000",
    "5,000",
    (
        "The Freely Extensible Biomedical Record Linkage (FEBRL) datasets consist"
        " of comparison patterns from an epidemiological cancer study in Germany."
        "FEBRL4b contains 5000 duplicate records, one for each record in FEBRL4a."
    ),
)
_ds_transactions_origin = _DataSetMetaData(
    "transactions_origin",
    f"{_splink_datasets_data_dir}/transactions_origin.parquet",
    "45,326",
    "45,326",
    (
        "This data has been generated to resemble bank transactions leaving an "
        "account. There are no duplicates within the dataset and each transaction"
        " is designed to have a counterpart arriving in 'transactions_destination'"
        ". Memo is sometimes truncated or missing."
    ),
    "parquet",
)
_ds_transactions_destination = _DataSetMetaData(
    "transactions_destination",
    f"{_splink_datasets_data_dir}/transactions_destination.parquet",
    "45,326",
    "45,326",
    (
        "This data has been generated to resemble bank transactions arriving in "
        "an account. There are no duplicates within the dataset and each "
        "transaction is designed to have a counterpart sent from "
        "'transactions_origin'. There may be a delay between the source and "
        "destination account, and the amount may vary due to hidden fees and "
        "foreign exchange rates. Memo is sometimes truncated or missing."
    ),
    "parquet",
)

_dsl_fake_1000 = _DataSetMetaData(
    "fake_1000_labels",
    f"{_splink_datasets_data_dir}/fake_1000_labels.csv",
    "3,176",
    "NA",
    ("Clerical labels for fake_1000"),
)

datasets = {
    "fake_1000": _ds_fake_1000,
    "historical_50k": _ds_historical_50k,
    "febrl3": _ds_febrl3,
    "febrl4a": _ds_febrl4a,
    "febrl4b": _ds_febrl4b,
    "transactions_origin": _ds_transactions_origin,
    "transactions_destination": _ds_transactions_destination,
}
dataset_labels = {
    "fake_1000": _dsl_fake_1000,
}
