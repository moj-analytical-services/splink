import os
import subprocess

import pandas as pd


def truncate_and_save(file_path, file_format):
    print(f"truncating file {file_path}")  # noqa: T201
    if file_format == "csv":
        truncated_file_path = os.path.splitext(file_path)[0] + "_truncated.csv"
        with open(truncated_file_path, "w") as truncated_file:
            subprocess.run(["head", "-n", "1001", file_path], stdout=truncated_file)
        os.rename(truncated_file_path, file_path)
    elif file_format == "parquet":
        df = pd.read_parquet(file_path)
        df = df.head(1000)
        df.to_parquet(file_path, index=False)


def process_directory(directory):
    print("Starting truncating files")  # noqa: T201
    for root, _, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            file_ext = os.path.splitext(file)[-1].lower()

            if file_ext == ".csv":
                truncate_and_save(file_path, "csv")
            elif file_ext == ".parquet":
                truncate_and_save(file_path, "parquet")

    for root, _, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            file_ext = os.path.splitext(file)[-1].lower()
            if file_ext == ".csv":
                df = pd.read_csv(file_path)
                print(f"length of {file_path} is {len(df)}")  # noqa: T201
            elif file_ext == ".parquet":
                truncate_and_save(file_path, "parquet")
                df = pd.read_parquet(file_path)
                print(f"length of {file_path} is {len(df)}")  # noqa: T201


if __name__ == "__main__":
    data_directory = "./data"
    process_directory(data_directory)
