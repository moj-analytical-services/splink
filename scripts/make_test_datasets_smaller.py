import os

import pandas as pd


def truncate_and_save(file_path, file_format):
    if file_format == "csv":
        df = pd.read_csv(file_path)
        df = df.head(1000)
        df.to_csv(file_path, index=False)
    elif file_format == "parquet":
        df = pd.read_parquet(file_path)
        df = df.head(1000)
        df.to_parquet(file_path)


def process_directory(directory):
    for root, _, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            file_ext = os.path.splitext(file)[-1].lower()

            if file_ext == ".csv":
                truncate_and_save(file_path, "csv")
            elif file_ext == ".parquet":
                truncate_and_save(file_path, "parquet")


if __name__ == "__main__":
    data_directory = "./data"
    process_directory(data_directory)
