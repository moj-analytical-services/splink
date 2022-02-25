import os
from tempfile import TemporaryDirectory

def dedupe_preserving_order(list_of_items):
    return list(dict.fromkeys(list_of_items))


def escape_columns(cols):
    return [escape_column(c) for c in cols]


def escape_column(col):
    return f"`{col}`"


def prob_to_bayes_factor(prob):
    return prob / (1 - prob)


def bayes_factor_to_prob(bf):
    return bf / (1 + bf)

def create_temp_folder():
    return TemporaryDirectory()

def create_db_folder(filepath, file_ext):
    file_extension = os.path.splitext(filepath)[1]
    if file_extension != file_ext:
        raise Exception(f"Error - {file_extension} is an invalid file extension.")

    filepath = os.path.split(filepath)
    if not os.path.exists(filepath[0]):
        os.mkdir(filepath[0])
